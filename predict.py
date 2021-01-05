import csv
import requests
import itertools
import random
import json
import datetime

#1) download minutely
#2) update predictions
#3) push to github

#where to scrape:
#NYT
#Georgia

#are mailin ballots processed uniformly? are other ballots processed uniformly?

#TODO: predict from a statewide estimate of remaining votes
#TODO: predict from a county-by-county estimate of remaining votes

#TODO: check how the nov 3 absentee file compared to the real turnout, also how much came in on the last day? Also how much is the absentee file adjusted later?

#High level:
#should have a good idea of early voting and mail-in turnout (a lot of precincts are within a few votes)
# - what's the day-of turnout?
# - how are each of the various categories voting compared to the last election?

def election_day_complete(results):
    #TODO: adjust as necessary -- one possibility is that this is returned by the georgia website when the precinct is marked as complete. Another possibility is just watching for when the precinct stops adding same day votes while continuing to count other votes
    pass

def election_day_turnout(precinct_data, precinct_completion, baseline_precinct_data, early_voting, projection_constants):
    #predict overall turnout on election day based on complete precincts
    #pull complete precincts
    baseline_total = 0

    real_sum = 0
    baseline_sum = 0

    for precinct in precinct_data:
        baseline_turnout = baseline_precinct_data[precinct]["Election Day Votes"]["total"]
        baseline_total += baseline_turnout
        if precinct_completion[precinct]:
            real_turnout = precinct_data[precinct]["Election Day Votes"]["total"]
            real_sum += real_turnout
            baseline_sum += baseline_turnout

    if baseline_sum:
        #total = (real_sum / baseline_sum) * baseline_total
        return (real_sum / baseline_sum)
    elif "election_day_ratio" in projection_constants:
        return projection_constants["election_day_ratio"]
    else:
        #fall back on the ratio of (early_turnout / baseline_turnout) for mail-ins
        early_sum = 0
        baseline_early_sum = 0
        for precinct in baseline_precinct_data:
            county_name, precinct_name = precinct
            if precinct_name in early_voting[county_name]["votes"]:
                baseline_early_sum += baseline_precinct_data[precinct]["Advanced Voting Votes"]["total"]
                early_sum += early_voting[county_name]["votes"][precinct_name]["total"]["Advanced Voting Votes"]
            else:
                print(f"Missing early voting data for: {str(precinct)}")
        print("early_sum, baseline_early, ratio: ", early_sum, baseline_early_sum, early_sum / baseline_early_sum)
        return (early_sum / baseline_early_sum)


def gen_turnout_estimates(precinct_data, precinct_completion, baseline_precinct_data, early_voting, projection_constants):
    election_day_ratio = election_day_turnout(precinct_data, precinct_completion, baseline_precinct_data, early_voting, projection_constants)

    #TODO: how to predict changes in turnout instead of just voters changing their minds?
    #for example, if there are two types of partisan districts, 90-10 rep and 90-10 dem
    #and no voters change their minds but democrats turn out +10% more voters than usual, while republicans vote normally
    #then you'll measure that the two types of districts are coming in more like 89-11 rep and 91-9 dem
    #which misses the major difference: the turnout in the two districts!
    #a few tactics:
    #- are there some vote types that are mixed together / counted at the county or state level instead of the precinct level? if so do predictions at that level
    #- once a few districts are complete, try to measure a correlation between partisanship and turnout of completed districts
    #- in so far as there's a correlation between voters changing their minds and turnout you could try to back out turnout from the margin difference
    #- could you even say that if dem-leaning districts have counted more votes than rep-leaning districts at time t then that predicts some difference in final turnout, even if none of the districts have stopped counting?
    for precinct in precinct_data:
        for category in precinct_data[precinct]:
            data = precinct_data[precinct][category]
            baseline_turnout = baseline_precinct_data[precinct][category]["total"]
            real_turnout = data["total"]
            if category == "Election Day Votes":
                if precinct_completion[precinct]: #think precinct completion only refers to counting election day votes
                    data["proj_total"] = data["total"]
                else:
                    data["proj_total"] = round(baseline_turnout * election_day_ratio) #round all projections
            elif category in ["Advanced Voting Votes", "Absentee by Mail Votes"]:
                county_name, precinct_name = precinct
                if precinct_name in early_voting[county_name]["votes"]:
                    data["proj_total"] = early_voting[county_name]["votes"][precinct_name]["total"][category]
                else:
                    print("No early data",precinct)
                    data["proj_total"] = 0
            else:
                data["proj_total"] = baseline_turnout
            data["proj_total"] = max(data["total"],data["proj_total"]) #can't project less than have already been counted


def get_category_projections(precinct_data, baseline_precinct_data):
    #can project a category a few different ways:
    #if votes are counted randomly then we can just compare everywhere
    #to expectations and add those up
    #if votes are counted very non-randomly you only want to compare
    #completed precincts to expectations
    #in general you could imagine weighting based on some monotonic
    #f(completion_percentage) with f(0) = 0 and f(1) = 1
    #with diff functions varying like
    #f(x) = 1, f(x) = x**0.5, f(x) = x, f(x) = x**2, f(x) = delta(1)
    weight_functions = {
        "all": lambda x: 1,
        "sqrt": lambda x: x**0.5,
        "linear": lambda x: x,
        "sq": lambda x: x**2,
        "complete": lambda x: 1 * (x==1)
    }

    #weighted total of how precincts are performing compared to baseline election
    #for example: mail-in ballots are +1% rep vs the baseline election
    category_margin_delta_sum = {}
    category_weight_sum = {}

    for precinct in precinct_data:
        for category in precinct_data[precinct]:
            baseline_data = baseline_precinct_data[precinct][category]
            data = precinct_data[precinct][category]

            #MUST: include rep_dem_share here
            margin_delta = data["margin_pct"] - baseline_data["margin_pct"]
            completion_pct = data["total"] / (data["proj_total"] + 1e-6)
            wt = weight_functions["linear"](completion_pct) * data["total"]

            category_margin_delta_sum[category] = category_margin_delta_sum.get(category,0) + (margin_delta * wt)
            category_weight_sum[category] = category_weight_sum.get(category,0) + wt
    output = {}
    for c in category_margin_delta_sum:
        margin_delta = category_margin_delta_sum[c] / (category_weight_sum[c] + 1e-6) #1e-6 to guess 0 for elections that haven't started
        output[c] = margin_delta
    return output


def gen_precinct_projections(precinct_data, baseline_precinct_data, category_projections, projection_constants):
    #once you have a generic projection you need to generate the
    #projection for each individual precinct
    #for the outstanding vote in the precinct, to what extent
    #should you rely on the projection versus the already
    #counted fraction?
    #again you can blend between the two weighting increasing
    #towards the counted information, maybe using the same
    #weight functions from category_projections?
    weight_functions = {
        "all": lambda x: 1,
        "sqrt": lambda x: x**0.5,
        "linear": lambda x: x,
        "sq": lambda x: x**2,
        "complete": lambda x: 1 * (x==1)
    }
    for precinct in precinct_data:
        for category in precinct_data[precinct]:
            live = precinct_data[precinct][category]
            baseline = baseline_precinct_data[precinct][category]

            completion_pct = live["total"] / (live["proj_total"] + 1e-6)
            margin_pct = live["margin_pct"]

            baseline_margin_pct = baseline["margin_pct"]
            proj_margin_pct = baseline_margin_pct + category_projections[category]
            wt = weight_functions["linear"](completion_pct)

            outstanding_margin_pct = wt * margin_pct + (1 - wt) * proj_margin_pct
            remaining_vote = live["proj_total"] - live["total"]

            #use baseline if there's no live data
            if live["rep_dem_share"]:
                rep_dem_share = live["rep_dem_share"]
            elif "rep_dem_share" in projection_constants:
                rep_dem_share = projection_constants["rep_dem_share"]
            else:
                rep_dem_share = baseline["rep_dem_share"]

            live["outs_rep"] = round((0.5 + outstanding_margin_pct / 2) * remaining_vote * rep_dem_share)
            live["outs_dem"] = round((0.5 - outstanding_margin_pct / 2) * remaining_vote * rep_dem_share)
            #prevent rounding errors that could make outs_rep + outs_dem > outs_total
            live["outs_total"] = max(remaining_vote,live["outs_rep"]+live["outs_dem"])
            live["proj_rep"] = live["rep"] + live["outs_rep"]
            live["proj_dem"] = live["dem"] + live["outs_dem"]
            live["proj_total"] = max(live["total"] + live["outs_total"], live["proj_total"]) #ensure proj_total >= total + outs_total, not sure if necessary

def read_election(filename, generate_test_data = False):
    #return election_data:
    #contest -> county -> "votes" -> precinct -> candidate -> category -> votes
    election_data = {}
    reader = csv.DictReader(open(filename))
    if generate_test_data:
        margin = 0 #-0.02 #simulate republicans getting 2% more margin
        frac_complete = 0 #random.random()
        print("frac_complete", frac_complete)
    for row in reader:
        contest = election_data.setdefault(row["contest"],{})
        county = contest.setdefault(row["county"].strip(),{})
        county_votes = county.setdefault("votes",{})
        precinct = county_votes.setdefault(row["precinct"].strip(),{})
        if generate_test_data:
            precinct["complete"] = 0
        else:
            precinct["complete"] = row["complete"]
        precinct_votes = precinct.setdefault("votes",{})
        candidate = precinct_votes.setdefault(row["candidate"],{})
        if candidate.get(row["category"],-1) < int(row["votes"]):
            county["georgia_timestamp"] = row["georgia_timestamp"]
            county["timestamp"] = row["timestamp"]
            county["version"] = row["version"]
            if generate_test_data:
                if "(Rep)" in row["candidate"]:
                    candidate[row["category"]] = frac_complete * int(row["votes"]) * (1 - margin)
                elif "(Dem)" in row["candidate"]:
                    candidate[row["category"]] = frac_complete * int(row["votes"]) * (1 + margin)
                else:
                    candidate[row["category"]] = frac_complete * int(row["votes"])
            else:
                candidate[row["category"]] = int(row["votes"])
    return election_data

def read_early_voting_data(id_, election_day_mmddyyyy, projection_constants):
    #returns early_voting:
    #county_name -> "votes" -> precinct -> "total" -> category -> cnt
    precinct_mapping = load_precinct_mapping() #map from absentee file to early voting
    reader = csv.DictReader(open(f"/home/jason/Downloads/{id_}/STATEWIDE.csv",encoding = "ISO-8859-1"))
    early_voting = {}
    unavailable = set()

    total_mail_count = 0
    election_day_mail_count = 0
    for row in reader:
        if row["Ballot Status"] == "A": #think this means accepted? there's also "C" = canceled, "S" = surrendered, "R" = rejected?
            county_name = " ".join([x.capitalize() for x in row["County"].split()]).replace(" ","_")
            county_name = {"Dekalb":"DeKalb", "Mcduffie":"McDuffie", "Mcintosh":"McIntosh"}.get(county_name,county_name)
            county = early_voting.setdefault(county_name,{})
            votes = county.setdefault("votes",{})
            if row["County Precinct"] in ["88888","99999"]: continue
            if (row["County"],row["County Precinct"]) in precinct_mapping:
                row["County Precinct"] = precinct_mapping[(row["County"],row["County Precinct"])]
            else:
                if (row["County"],row["County Precinct"]) not in unavailable:
                    print("Unknown early voting precinct, won't be processed: ",row["County"],row["County Precinct"])
                    unavailable.add((row["County"],row["County Precinct"]))
            precinct = county["votes"].setdefault(row["County Precinct"],{})
            totals = precinct.setdefault("total",{})

            totals.setdefault("Absentee by Mail Votes",0)
            totals.setdefault("Advanced Voting Votes",0)
            if row["Ballot Style"] == "MAILED":
                totals["Absentee by Mail Votes"] += 1

                total_mail_count += 1
                if row["Ballot Return Date"] == election_day_mmddyyyy:
                    election_day_mail_count += 1
            elif row["Ballot Style"] == "IN PERSON":
                totals["Advanced Voting Votes"] += 1

    #adjust mailin counts upward to account for ballots that haven't been received
    #second clause of the if statement to automatically turn off this adjustment if/when we have an early vote file
    #that includes the election day mail count (probably will arrive in the middle of the night of the election)
    if "election_day_mail_count" in projection_constants and election_day_mail_count < 0.25 * projection_constants["election_day_mail_count"]:
        outstanding_election_day_count = projection_constants["election_day_mail_count"] - election_day_mail_count
        multiplier = (total_mail_count + outstanding_election_day_count) / total_mail_count #adjust all by this factor
        for county_name in early_voting:
            for precinct_name in early_voting[county_name]["votes"]:
                vote_counts = early_voting[county_name]["votes"][precinct_name]["total"]
                vote_counts["Absentee by Mail Votes"] = round(vote_counts["Absentee by Mail Votes"] * multiplier)

    return early_voting

def load_precinct_mapping():
    #precinct_mapping.csv file originally generated from the
    #https://www2.census.gov/geo/docs/reference/codes/files/st13_ga_vtd.txt file along with a lot of tweaks
    #download_precinct_mapping_baseline() downloads the census.gov baseline mapping
    #debug_precinct_mapping() helper function for making the tweaks to the baseline mapping
    reader = csv.DictReader(open("precinct_mapping.csv"))
    mapping = {}
    for row in reader:
        mapping[(row["county"],row["label"])] = row["precinct"]
    return mapping

def download_precinct_mapping_baseline():
    #used to generate precinct_mapping.csv
    data = requests.get("https://www2.census.gov/geo/docs/reference/codes/files/st13_ga_vtd.txt").text
    all_rows = [x.strip() for x in data.split("\n") if x.strip()]
    mapping = {}
    for row in all_rows:
        county = row.split("|")[3].replace("County","").strip().upper()
        precinct_short = row.split("|")[4].strip()
        precinct_full = row.split("|")[5].split("-")[1].replace("Voting District","").strip()
        precinct_full = " ".join([x.capitalize() if x.isalpha() else x for x in precinct_full.split(" ")])
        mapping[(county,precinct_short)] = precinct_full
    return mapping

def debug_precinct_mapping():
    precinct_mapping = load_precinct_mapping()
    early_voting = read_early_voting_data(35209,"11/03/2020",{}) #nov 3 id
    election_data = read_election("/tmp/election_results_nov_3.csv")
    for x in election_data:
        contest = election_data[x]
        for county in contest:
            for precinct in contest[county]["votes"]:
                test = {}
                for p in contest[county]["votes"]:
                    test.setdefault(p,{})
                    for c in contest[county]["votes"][p]["votes"]:
                        for t in contest[county]["votes"][p]["votes"][c]:
                            test[p][t] = test[p].get(t,0) + contest[county]["votes"][p]["votes"][c][t]
                early_mismatches = sorted([(x,early_voting[county]["votes"][x]["total"]["Advanced Voting Votes"]) for x in early_voting[county]["votes"] if x not in contest[county]["votes"]], key = lambda x: x[1])
                contest_mismatches = sorted([(x,test[x]["Advanced Voting Votes"]) for x in contest[county]["votes"] if x not in early_voting[county]["votes"]], key = lambda x: x[1])

                if county == "Fulton" and precinct in ['CP084', 'SC08H', 'UC01D', '01I']: continue #no early votes, only 0-1 votes in the general election data
                if len(early_mismatches) and len(contest_mismatches):
                    reverse_lookups = {}
                    for x in precinct_mapping:
                        if x[0] == county.upper():
                            print("test", x, precinct_mapping[x])
                            if precinct_mapping[x] in [x[0] for x in early_mismatches]:
                                print(x,precinct_mapping[x])
                                reverse_lookups[precinct_mapping[x]] = x[1]
                    print("zips")
                    print("---")
                    for x1, x2 in itertools.zip_longest(early_mismatches, contest_mismatches):
                        print(x1, x2)
                    print("mapping help")
                    print("---")
                    for x1, x2 in zip(early_mismatches, contest_mismatches):
                        print(f"mapping[('{county.upper()}','{reverse_lookups.get(x1[0],x1[0])}')] = '{x2[0]}'")
                    raise
                election_precinct_info = contest[county]["votes"][precinct]["votes"]
                early_precinct_info = early_voting[county]["votes"][precinct]
                for category in early_precinct_info["total"]:
                    projected_votes = early_precinct_info["total"][category]
                    actual_votes = sum([election_precinct_info[candidate][category] for candidate in election_precinct_info])
                    if ((projected_votes - actual_votes) / (actual_votes + 1e-6)) > 0.2:
                        print("Early versus actual discrepancy: ", county, precinct, category, projected_votes, actual_votes)


def preprocess_election_data(election_data):
    data = {}
    for county in election_data:
        for precinct in election_data[county]["votes"]:
            data.setdefault((county,precinct),{})
            precinct_data = election_data[county]["votes"][precinct]["votes"]
            cnt = {}
            rep = {}
            dem = {}
            for candidate in precinct_data:
                for category in precinct_data[candidate]:
                    if "(Rep)" in candidate:
                        rep[category] = rep.get(category,0) + precinct_data[candidate][category]
                    if "(Dem)" in candidate:
                        dem[category] = dem.get(category,0) + precinct_data[candidate][category]
                    cnt[category] = cnt.get(category,0) + precinct_data[candidate][category]
            for category in cnt:
                data[(county,precinct)].setdefault(category,{})
                data[(county,precinct)][category]["margin_pct"] = (rep[category]-dem[category]) / (rep[category]+dem[category]+1e-6)
                data[(county,precinct)][category]["rep"] = rep[category]
                data[(county,precinct)][category]["dem"] = dem[category]
                data[(county,precinct)][category]["total"] = cnt[category]
                data[(county,precinct)][category]["rep_dem_share"] = (rep[category]+dem[category]) / (cnt[category] + 1e-6)
    return data

def combine_early_baseline_live(early_voting, baseline_election_data, live_election_data, projection_constants):
    baseline_data = preprocess_election_data(baseline_election_data)
    live_data = preprocess_election_data(live_election_data)

    #specify precincts as completed
    precinct_completion = {}
    for county in live_election_data:
        for precinct in live_election_data[county]["votes"]:
            precinct_completion[(county,precinct)] = live_election_data[county]["votes"][precinct]["complete"]

    #use loaded data for projections
    gen_turnout_estimates(live_data, precinct_completion, baseline_data, early_voting, projection_constants)

    #TODO: optionally overwrite completion_estimates with a certain amount outstanding by county or by category statewide
    #as sometimes this information comes in towards the end

    category_projections = get_category_projections(live_data, baseline_data)
    print("category_projections",category_projections)

    gen_precinct_projections(live_data, baseline_data, category_projections, projection_constants)

    export_fields = ["rep","dem","total","outs_rep","outs_dem","outs_total","proj_rep","proj_dem","proj_total"]

    #aggregate to county-level, state-level, including an extra aggregate category "total"
    state_data = {}
    county_data = {}
    for county,precinct in live_data:
        data = live_data[(county,precinct)]
        county_data.setdefault(county,{})

        #create special "total" category
        county_data[county].setdefault("total",{})
        state_data.setdefault("total",{})
        data.setdefault("total",{})

        for category in data.keys():
            if category == "total": continue #total needs special treatment
            #cleanup precinct data to remove extra fields
            data[category] = {x:data[category][x] for x in data[category] if x in export_fields}
            for field in export_fields:
                state_data["total"][field] = state_data["total"].get(field,0) + data[category][field]
                state_data.setdefault(category,{})
                state_data[category][field] = state_data[category].get(field,0) + data[category][field]

                county_data[county]["total"][field] = county_data[county]["total"].get(field,0) + data[category][field]
                county_data[county].setdefault(category,{})
                county_data[county][category][field] = county_data[county][category].get(field,0) + data[category][field]

                data["total"][field] = data["total"].get(field,0) + data[category][field]

    #convert (county, precinct) tuple keys to "county|precinct" string for json export
    precinct_data = {"|".join(x):live_data[x] for x in live_data}
    return {"precinct": precinct_data, "county": county_data, "state": state_data, "time": datetime.datetime.now().strftime("%-I:%M %p ET, %B %-d, %Y")}

def generate_predictions():
    #specify projection_constants for expected differences from the baseline election
    #general priority:
    #1) live data if it exists
    #2) projection constants if specified
    #3) data from baseline election

    #MUST: how many more mail-in ballots are expected?
    #Nate Cohn saying something like 100k
    #maybe look at the pattern from the nov 3 file on the days leading up to the election?

    #MUST: on Jan 3 Nate Cohn estimates "[...] Ossoff will amass a lead of around 350k out of the advance vote, including what he'll net out of ~100k absentee votes still to arrive"
    #where does the 100k estimate come from? are mail-in ballots that arrive late allowed?

    projection_constants = {
        "rep_dem_share": 1, #assuming everyone in the runoff will vote for one of the two candidates, website said no write-ins allowed
        "election_day_ratio": 0.81, #early voting is at 0.77 as of Jan 4 but maybe it would have been higher if not for the holidays, Nate Cohn suggesting 0.81 here: https://twitter.com/Nate_Cohn/status/1345766439135948801
        "election_day_mail_count": 0.83 * 42718, #spitball estimate of the same-day mailin vote to be received on election day as 83% of that received for the general election -- rough extrapolation from looking at the amount coming in each day in early_voting_trends.py
    }

    early_voting = read_early_voting_data(35211, "01/05/2021", projection_constants) #jan 5 runoff #MUST: download latest version on election day

    #generate a smaller dict with county and statewide info and a larger one with all precinct data
    pred = {}
    precinct_pred = {}

    #using perdue as baseline for both, as the loeffler nov 3 election had many candidates so not as representative
    contests = [("perdue", "perdue"), ("perdue", "loeffler")]
    for [baseline, live] in contests:
        print("---")
        print("---")
        print("---")
        print(f"Processing {live} election")
        baseline_election_data = read_election("/tmp/election_results_nov_3.csv")[baseline]

        TESTING = True #MUST: change to False and generate /tmp/election_results_jan_5.csv
        if TESTING:
            test_data = True #(live == "loeffler")
            live_election_data = read_election("/tmp/election_results_nov_3.csv", test_data)["perdue"]
        else:
            live_election_data = read_election("/tmp/election_results_jan_5.csv")[live]

        data = combine_early_baseline_live(early_voting, baseline_election_data, live_election_data, projection_constants)

        pred.setdefault(live, {})
        pred[live]["county"] = data["county"]
        pred[live]["state"] = data["state"]
        pred["time"] = data["time"]
        precinct_pred[live] = data["precinct"]


    with open("pred.json","w") as f_out:
        f_out.write(json.dumps(pred));
    with open("precinct-pred.json","w") as f_out:
        f_out.write(json.dumps(precinct_pred));

if __name__ == "__main__":
    #debug_precinct_mapping()
    generate_predictions()
