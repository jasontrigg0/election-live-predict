#pull results from Georgia's website, by precinct
import csv
from bs4 import BeautifulSoup
import requests
import json
import io
import zipfile
import datetime
import itertools
import os
import time

def get(url, options=None):
    delay = 1
    while True:
        try:
            return requests.get(url, options)
        except:
            time.sleep(delay)
            delay *= 1.25
            if delay > 60:
                raise

def all_county_info(election_id):
    current_version = get(f"https://results.enr.clarityelections.com//GA/{election_id}/current_ver.txt").text
    url = f"https://results.enr.clarityelections.com//GA/{election_id}/{current_version}/json/en/electionsettings.json"
    r = get(url)

    for county_string in json.loads(r.text)["settings"]["electiondetails"]["participatingcounties"]:
        county, county_election_id, version, georgia_timestamp, _ = county_string.split("|")
        yield {
            "county": county,
            "county_election_id": county_election_id,
            #"version": version, #don't use the version from the main page, as it can go out of date -- instead pull from the county itself
            "georgia_timestamp": georgia_timestamp
        }

def scrape_general_election_results(election_id, latest_county_versions):
    for county_info in all_county_info(election_id):
        #sometimes scrape_county requests return invalid json which then breaks
        #so retry the same county when that happens after waiting for 5 seconds
        while True:
            try:
                rows = list(scrape_county(county_info, latest_county_versions.get(county_info["county"],-1)))
                break
            except json.decoder.JSONDecodeError:
                print("Download error. Sleeping for 5 seconds and retrying...")
                time.sleep(5)
        for r in rows:
            yield r

def scrape_county(county_info, latest_version):
    #get version from the county page instead of the state page as it's more up-to-date
    county_info["version"] = get(f"https://results.enr.clarityelections.com//GA/{county_info['county']}/{county_info['county_election_id']}/current_ver.txt").text

    #only download for counties with new updates available
    if int(county_info["version"]) <= int(latest_version):
        return []
    #pull county precinct info:
    dat = json.loads(get(f"https://results.enr.clarityelections.com//GA/{county_info['county']}/{county_info['county_election_id']}/{county_info['version']}/json/status.json").text)
    if not len(dat["P"]) == len(dat["S"]):
        raise
    completion = {p:1*(s==4) for p,s in zip(dat["P"],dat["S"])} #TODO: confirm this works for not-started precincts pre-election

    #NOTE: as of Jan 5, 11:25 AM looks like xml isn't available? Using scrape_county_results_json instead
    for row in scrape_county_results_json(county_info):
        row["complete"] = completion[row["precinct"]]
        yield row

def scrape_county_results_json(county_info):
    county = county_info["county"]
    county_election_id = county_info["county_election_id"]
    version = county_info["version"]
    #we want breakdowns by (category_of_vote,precinct)
    #ignoring these json files:
    #details.json, which gives breakdown by precinct summed across categories
    #all.json, which gives breakdown by precinct summed across categories (how's that different from details.json?)
    #vt.json, which gives breakdown of categories summed across precincts
    #sum.json which sums across categories and precincts
    #summary.json other than contest ids (also sums across categories and precincts)

    summary = json.loads(get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/en/summary.json").text)
    print(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/en/summary.json")
    id_to_contest = {x["K"]:x["C"] for x in summary}
    id_to_candidates = {x["K"]:x["CH"] for x in summary}

    settings = json.loads(get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/en/electionsettings.json").text)
    georgia_timestamp = settings["websiteupdatedat"]

    download_time = datetime.datetime.now()

    vote_types = {
        "Election Day Votes": json.loads(get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/Election_Day_Votes.json").text),
        "Absentee by Mail Votes": json.loads(get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/Absentee_by_Mail_Votes.json").text),
        "Advanced Voting Votes": json.loads(get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/Advanced_Voting_Votes.json").text),
        "Provisional Votes": json.loads(get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/Provisional_Votes.json").text)
    }

    for type_ in vote_types:
        info = vote_types[type_]
        for x in info["Contests"]:
            precinct = x["A"]
            if precinct == "-1": continue #placeholder precinct with totals for the county
            for contest_id, all_votes in zip(x["C"],x["V"]):
                contest = id_to_contest[contest_id]

                candidate_list = id_to_candidates[contest_id]
                for candidate, vote_count in zip(candidate_list, all_votes):
                    yield {
                        "contest": contest,
                        "county": county,
                        "precinct": precinct,
                        "georgia_timestamp": georgia_timestamp,
                        "timestamp": download_time,
                        "version": version,
                        "candidate": candidate,
                        "category": type_,
                        "votes": vote_count
                    }


def scrape_county_results_xml(county_info):
    download_time = datetime.datetime.now()
    url = f"https://results.enr.clarityelections.com//GA/{county_info['county']}/{county_info['county_election_id']}/{county_info['version']}/reports/detailxml.zip"
    r = get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    if not "detail.xml" in z.namelist():
        raise
    raw_xml = z.read("detail.xml")
    soup = BeautifulSoup(raw_xml, "lxml")
    georgia_timestamp = soup.select("timestamp")[0].text
    for contest in soup.select('contest'):
        for choice in contest.select('choice'):
            for votetype in choice.select("votetype"):
                for precinct in votetype.select("precinct"):
                    yield {
                        "contest": contest["text"],
                        "county": county_info["county"],
                        "georgia_timestamp": georgia_timestamp,
                        "timestamp": download_time,
                        "version": county_info["version"],
                        "precinct": precinct["name"],
                        "candidate": choice["text"],
                        "category": votetype["name"],
                        "votes": precinct["votes"]
                    }

def read_general_election_history():
    results = {}
    for r in csv.DictReader("general_election.csv"):
        yield r

def read_runoff_history():
    results = {}
    for r in csv.DictReader("runoff.csv"):
        yield r

def update_election_data(election_id, filename, contest_name_mapping):
    #first read in existing rows, then scrape current data and check for new rows
    county_versions = {}

    file_exists = os.path.exists(filename)

    #read existing data
    if file_exists:
        reader = csv.DictReader(open(filename))
        for row in reader:
            if int(row["version"]) > int(county_versions.get(row["county"],-1)):
                county_versions[row["county"]] = row["version"]


    #scrape new data
    writer = csv.DictWriter(open(filename,"a"),fieldnames=["contest","county","georgia_timestamp","timestamp","version","precinct","complete","candidate","category","votes"])

    #write header only when there's no existing data
    if not file_exists:
        writer.writeheader()

    found_new_data = False
    for row in scrape_general_election_results(election_id, county_versions):
        found_new_data = True
        #scrape only perdue for nov 3, as the loeffler election had many candidates so isn't very representative
        #Gwinnett county uses a different format for the contest names
        if row["contest"] not in contest_name_mapping: continue
        row["contest"] = contest_name_mapping[row["contest"]]
        writer.writerow(row)
    return found_new_data


def update_nov_3_election_data():
    contest_name_mapping = {
        "US Senate (Perdue)": "perdue",
        "US Senate (Perdue)/Senado de los EE.UU. (Perdue)": "perdue"
    }
    return update_election_data(105369, "/tmp/election_results_nov_3.csv", contest_name_mapping)

def update_jan_5_election_data():
    #would normally find election info through the homepage:
    #https://sos.ga.gov/index.php/Elections/current_and_past_elections_results
    #but as of ~12p on election day they weren't linking it in advance
    #however googling found it listed here: https://results.enr.clarityelections.com/GA/Thomas/
    #maybe could've pinged every election id to find it otherwise?
    #or waited for the link, though the prep time is helpful
    contest_name_mapping = {
        "US Senate (Perdue)": "perdue",
        "US Senate (Loeffler) - Special": "loeffler",
        "US Senate (Perdue)/Senado de los EE.UU. (Perdue)": "perdue",
        "US Senate (Loeffler) - Special/Senado de los EE.UU. (Loeffler) - Especial": "loeffler"
    }
    return update_election_data(107556, "/tmp/election_results_jan_5.csv", contest_name_mapping)

def scrape_betfair_odds():
    cookies = {
        '__cfduid': 'db5973faceaefe3c7c2eb24ee88d049c61609859066',
        'xsrftoken': '4dcb3180-4f67-11eb-8d10-fa163e3852cd',
        'wsid': '4dcb3181-4f67-11eb-8d10-fa163e3852cd',
        'vid': '8ac21c16-e0cb-451f-95af-9481c379ad1c',
        'betexPtk': 'betexCurrency%3DGBP%7EbetexLocale%3Den%7EbetexRegion%3DGBR',
        'betexPtkSess': 'betexCurrencySessionCookie%3DGBP%7EbetexLocaleSessionCookie%3Den%7EbetexRegionSessionCookie%3DGBR',
        'bfsd': 'ts=1609859068317|st=p',
        '_gcl_au': '1.1.1188426924.1609859069',
        'storageSSC': 'lsSSC%3D1',
        'exp': 'sb',
        'PI': '3013',
        'StickyTags': 'rfr=3013',
        'TrackingTags': '',
        'pi': 'partner3013',
        'rfr': '3013',
        'OptanonConsent': 'isIABGlobal=false&datestamp=Tue+Jan+05+2021+10%3A25%3A48+GMT-0500+(Eastern+Standard+Time)&version=6.6.0&hosts=&consentId=c86f7ba8-f0a3-43b5-90ae-6a158156c7a4&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&geolocation=%3B&AwaitingReconsent=false',
        'OptanonAlertBoxClosed': '2021-01-05T15:05:24.987Z',
        'Qualtrics_Cookie': '123456',
        '_uetsid': '49c06f904f6a11ebb6f6f7551cb7bf67',
        '_uetvid': '49c090804f6a11eb969d37159970e2ff',
        '_scid': '91ba6b32-4835-4a14-8118-f69684d8fb0d',
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.5',
        'Content-Type': 'application/json',
        'X-Application': 'FIhovAzZxtrvphhu',
        'Origin': 'https://www.betfair.com',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Referer': 'https://www.betfair.com/',
        'TE': 'Trailers',
    }

    params = (
        ('xsrftoken', '4dcb3180-4f67-11eb-8d10-fa163e3852cd'),
        ('_ak', 'FIhovAzZxtrvphhu'),
        ('priceHistory', '0'),
    )

    data = '{"alt":"json","locale":"en_GB","currencyCode":"GBP","marketIds":["924.246442444","924.245218724"]}'

    response = requests.post('https://smp.betfair.com/www/sports/fixedodds/readonly/v1/getMarketPrices', headers=headers, params=params, cookies=cookies, data=data)

    id_to_name = {
        37071217: "perdue",
        37071216: "ossoff",
        36786003: "warnock",
        36786002: "loeffler"
    }

    response_data = [{"candidate":id_to_name[y["selectionId"]], "odds": round(y["runnerOdds"]["trueOdds"]["decimalOdds"]["decimalOdds"],4)} for x in json.loads(response.text) for y in x["runnerDetails"]]

    writer = csv.DictWriter(open("betfair.csv","a"),fieldnames=["time","candidate","odds"])
    for data in response_data:
        writer.writerow({"time": datetime.datetime.now(), "candidate": data["candidate"], "odds": data["odds"]})


if __name__ == "__main__":
    #scrape_betfair_odds()
    update_jan_5_election_data()
