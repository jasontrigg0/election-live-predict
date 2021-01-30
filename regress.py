#little test script to generate regression data. may need fixing (as of 2021-01-27)
import csv
import sys
import os
sys.path.append(os.path.dirname(__file__)) #allow import from current directory
from predict import read_early_voting_data
import datetime


if __name__ == "__main__":
    info = {}

    projection_constants = {
        "rep_dem_share": 1, #assuming everyone in the runoff will vote for one of the two candidates, website said no write-ins allowed
        "election_day_ratio": 1.25, #early voting is at 0.77 as of Jan 4 but maybe it would have been higher if not for the holidays, Nate Cohn suggesting 0.81 here: https://twitter.com/Nate_Cohn/status/1345766439135948801
        "election_day_mail_count": 0.83 * 42718, #spitball estimate of the same-day mailin vote to be received on election day as 83% of that received for the general election -- rough extrapolation from looking at the amount coming in each day in early_voting_trends.py
    }

    early_voting = read_early_voting_data(35211, "01/05/2021", projection_constants)

    load_election("baseline", "/tmp/election_results_nov_3.csv", info)
    load_election("current", "election_results_jan_5.csv", info)
    writer = csv.DictWriter(open("/tmp/precincts.csv","w"),fieldnames=["county","precinct","category","nov_rep_cnt","nov_dem_cnt","nov_cnt","nov_version","jan_rep_cnt","jan_dem_cnt","jan_cnt","jan_version", "early_cnt"])
    writer.writeheader()
    for precinct in info:
        for category in info[precinct]:
            row = info[precinct][category]
            row["county"] = precinct[0]
            row["precinct"] = precinct[1]
            early_county = early_voting.get(row["county"],{}).get("votes",{})
            early = early_county.get(row["precinct"],{}).get("total",{})
            if category in early:
                row["early_cnt"] = early[category]
            else:
                row["early_cnt"] = ""
            row["category"] = category
            writer.writerow(row)
