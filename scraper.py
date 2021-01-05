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
            "version": version,
            "georgia_timestamp": georgia_timestamp
        }

def scrape_general_election_results(election_id, county_versions):
    for county_info in all_county_info(election_id):
        #only download for counties with new updates available
        if int(county_info["version"]) <= int(county_versions.get(county_info["county"],-1)):
            continue
        #pull county precinct info:
        dat = json.loads(get(f"https://results.enr.clarityelections.com//GA/{county_info['county']}/{county_info['county_election_id']}/{county_info['version']}/json/status.json").text)
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

    #read existing data
    if os.path.exists(filename):
        reader = csv.DictReader(open(filename))
        for row in reader:
            if int(row["version"]) > int(county_versions.get(row["county"],-1)):
                county_versions[row["county"]] = row["version"]


    #scrape new data
    writer = csv.DictWriter(open(filename,"a"),fieldnames=["contest","county","georgia_timestamp","timestamp","version","precinct","complete","candidate","category","votes"])

    #write header only when there's no existing data
    if not os.path.exists(filename):
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
    #MUST: fill in election id and contest_name_mapping
    contest_name_mapping = {

    }
    return update_election_data(None, "/tmp/election_results_jan_5.csv", contest_name_mapping)

if __name__ == "__main__":
    update_nov_3_election_data()
