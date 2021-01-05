#try scraping from a few different sources
#Georgia, NYT, other?
import time
import csv
from bs4 import BeautifulSoup
import requests
import json
import io
import zipfile
import datetime

#TODOs
#read existing file of results
#scrape every minute, on updates write to file


def all_county_info(election_id):
    current_version = requests.get(f"https://results.enr.clarityelections.com//GA/{election_id}/current_ver.txt").text
    url = f"https://results.enr.clarityelections.com//GA/{election_id}/{current_version}/json/en/electionsettings.json"
    r = requests.get(url)

    for county_string in json.loads(r.text)["settings"]["electiondetails"]["participatingcounties"]:
        county, county_election_id, version, georgia_timestamp, _ = county_string.split("|")
        yield {
            "county": county,
            "county_election_id": county_election_id,
            "version": version,
            "georgia_timestamp": georgia_timestamp
        }

def scrape_general_election_results(election_id):
    for county_info in all_county_info(election_id):
        #pull county precinct info:
        dat = json.loads(requests.get(f"https://results.enr.clarityelections.com//GA/{county_info['county']}/{county_info['county_election_id']}/{county_info['version']}/json/status.json").text)
        if not len(dat["P"]) == len(dat["S"]):
            raise
        completion = {p:1*(s==4) for p,s in zip(dat["P"],dat["S"])} #TODO: confirm this works for not-started precincts pre-election

        for row in scrape_county_results_xml(county_info):
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

    summary = json.loads(requests.get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/en/summary.json").text)
    print(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/en/summary.json")
    id_to_contest = {x["K"]:x["C"] for x in summary}
    id_to_candidates = {x["K"]:x["CH"] for x in summary}

    settings = json.loads(requests.get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/en/electionsettings.json").text)
    georgia_timestamp = settings["websiteupdatedat"]

    download_time = datetime.datetime.now()

    vote_types = { #MUST: confirm keys here match the votetype names in the other function
        "Election Day Votes": json.loads(requests.get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/Election_Day_Votes.json").text),
        "Absentee by Mail Votes": json.loads(requests.get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/Absentee_by_Mail_Votes.json").text),
        "Advanced Voting Votes": json.loads(requests.get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/Advanced_Voting_Votes.json").text),
        "Provisional Votes": json.loads(requests.get(f"https://results.enr.clarityelections.com//GA/{county}/{county_election_id}/{version}/json/Provisional_Votes.json").text)
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
    r = requests.get(url)
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

def scrape_nov_3():
    #scrape only perdue for nov 3, as the loeffler election had many candidates so isn't very representative
    nov_3_election_id = 105369
    writer = csv.DictWriter(open("/tmp/election_results_nov_3.csv","w"),fieldnames=["contest","county","georgia_timestamp","timestamp","version","precinct","complete","candidate","category","votes"])
    writer.writeheader()
    for row in scrape_general_election_results(nov_3_election_id):
        contest_name_mapping = {
            "US Senate (Perdue)": "perdue",
            "US Senate (Perdue)/Senado de los EE.UU. (Perdue)": "perdue"
        }
        #Gwinnett county uses a different format for the contest names
        if row["contest"] not in contest_name_mapping: continue
        row["contest"] = contest_name_mapping[row["contest"]]
        writer.writerow(row)

def scrape_jan_5():
    jan_5_election_id = None #MUST: fill in
    writer = csv.DictWriter(open("/tmp/election_results_jan_5.csv","w"),fieldnames=["contest","county","georgia_timestamp","timestamp","version","precinct","complete","candidate","category","votes"])
    writer.writeheader()
    for row in scrape_general_election_results(jan_5_election_id):
        #MUST: add perdue and loeffler contest names, plus possibly also double check whether Gwinnett county uses a different name
        contest_name_mapping = {

        }
        if row["contest"] not in contest_name_mapping: continue
        row["contest"] = contest_name_mapping[row["contest"]]
        writer.writerow(row)


#pull results from Georgia's website, by precinct
if __name__ == "__main__":
    #loop through, continuously updating
    scrape_nov_3()
