#regression-based predictions, should be more accurate + extensible than the original more heuristic-y version
#maybe the most important variable to tweak here is the regularization alpha - it's set to 25, could try higher or lower
#also priors could be tweaks, especially for the intercept to force a reasonable prediction before any data comes in
#a couple todos
#this estimates early turnout from the absentee file, but early voting turnout ended up greater: how to predict this as
#the evening goes on? a problem here is telling when a given precinct is done counting.
import sys
import os
sys.path.append(os.path.dirname(__file__)) #allow import from current directory
from lasso import StandardLasso
import pandas as pd
import numpy as np
import pickle
import datetime
import json
import csv
import glob

def get_primary_voting():
    #TODO: possibly try including information about whether they returned their ballot
    #in the primary?? currently only tracking which primary each voter voted in
    info = {}
    for filename in glob.glob("/home/jason/Downloads/35212/*csv"):
        reader = csv.DictReader(open(filename))
        for row in reader:
            info[row["Voter Registration #"]] = row["Party"]
    return info

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
        if candidate.get(row["version"],-1) < int(row["version"]):
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

def read_early_voting_data(id_, election_day_mmddyyyy, projection_constants, primary_data = None):
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
            rep_primary = precinct.setdefault("rep_primary",{})
            dem_primary = precinct.setdefault("dem_primary",{})

            #initialize
            for dict_ in [totals,rep_primary,dem_primary]:
                for typename in ["Absentee by Mail Votes", "Advanced Voting Votes"]:
                    dict_.setdefault(typename,0)

            if row["Ballot Style"] == "MAILED":
                typename = "Absentee by Mail Votes"
                totals[typename] += 1
                if primary_data and row["Voter Registration #"] in primary_data:
                    if primary_data[row["Voter Registration #"]] == "REPUBLICAN":
                        rep_primary[typename] += 1
                    elif primary_data[row["Voter Registration #"]] == "DEMOCRAT":
                        dem_primary[typename] += 1

                total_mail_count += 1
                if row["Ballot Return Date"] == election_day_mmddyyyy:
                    election_day_mail_count += 1
            elif row["Ballot Style"] == "IN PERSON":
                typename = "Advanced Voting Votes"
                totals[typename] += 1
                if primary_data and row["Voter Registration #"] in primary_data:
                    if primary_data[row["Voter Registration #"]] == "REPUBLICAN":
                        rep_primary[typename] += 1
                    elif primary_data[row["Voter Registration #"]] == "DEMOCRAT":
                        dem_primary[typename] += 1


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

def load_election(info, filename, contest, prefix, max_time=None):
    reader = csv.DictReader(open(filename))
    for row in reader:
        if max_time and row["timestamp"] > max_time: continue
        if row["contest"] != contest: continue
        county_info = info.setdefault(row["county"],{})
        precinct_info = county_info.setdefault(row["precinct"].strip(),{})
        category_info = precinct_info.setdefault(row["category"],{})

        #Track what time the election day votes came in
        #note: this will record the last time election day votes were counted
        #but it looked like the large majority of precincts have all
        #election day votes come in at once
        # time = datetime.datetime.strptime(row["georgia_timestamp"], "%m/%d/%Y %I:%M:%S %p EST")
        # time = max(time, datetime.datetime(2021,1,5,19,0,0))
        # time = min(time, datetime.datetime(2021,1,6,0,0,0))
        # t = (time - datetime.datetime(2021,1,5,21,30,0)).total_seconds()
        # new_rep = ("(Rep)" in row["candidate"]) and (int(row["votes"]) > category_info.get(f"{prefix}rep",0))
        # new_dem = ("(Dem)" in row["candidate"]) and (int(row["votes"]) > category_info.get(f"{prefix}dem",0))
        # if row["category"] == "Election Day Votes" and (new_rep or new_dem):
        #     category_info[f"{prefix}time"] = t

        if not category_info.get(f"{prefix}version",None) or row["version"] > category_info[f"{prefix}version"]:
            category_info[f"{prefix}total"] = 0
            category_info[f"{prefix}version"] = row["version"]

        if "(Rep)" in row["candidate"]:
            category_info[f"{prefix}rep"] = int(row["votes"])
        if "(Dem)" in row["candidate"]:
            category_info[f"{prefix}dem"] = int(row["votes"])


        category_info[f"{prefix}total"] += int(row["votes"])


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

def load_county_demographics():
    reader = csv.DictReader(open("georgia_county_demographics.csv"))
    data = {}
    for row in reader:
        data[row["county"].replace(" ","_")] = row
    return data

def dataframe_to_nested_dict(df):
    if not isinstance(df.index, pd.MultiIndex):
        return df.to_dict('index')
    elif len(df.index.levels) == 2:
        return {level0: {level1: dict(df.xs([level0,level1])) if isinstance(df.xs([level0, level1]), pd.Series) else df.xs([level0, level1]).to_dict('index') for level1 in df.index.levels[1]} for level0 in df.index.levels[0]}
    else:
        raise

def group_by_category_with_total_and_sum(df, fields, cols):
    #utility function for grouping by: fields + [category]
    #but to also include a 'total' category, which is the sum across all regular categories

    #dataframe with the sums across regular categories
    df1 = df.groupby(fields + ['category'])[cols].agg('sum')

    #dataframe with the sum across all categories
    #special casing needed here because df.groupby([]) throws an error instead of just summing across all rows
    if fields:
        df2 = df.groupby(fields)[cols].agg('sum')
    else:
        df2 = pd.DataFrame().append(df[cols].agg('sum'),ignore_index=True)
    df2['category'] = 'total'
    df2 = df2.groupby(fields + ['category'])[cols].agg('sum')

    return pd.concat([df1,df2])

def compute_cnts(info):
    #compute county and overall stats *for completed precincts only*
    county_cnts = {}
    state_cnts = {}
    for county in info:
        for precinct in info[county]:
            for category in info[county][precinct]:
                row = info[county][precinct][category]

                row["total"] = row.get("total",0)
                row["rep"] = row.get("rep",0)
                row["dem"] = row.get("dem",0)

                if not row["total"]: continue #TODO: replace with better measure of whether the precinct is complete

                county_info = county_cnts.setdefault(county,{}).setdefault(category,{})
                county_info.setdefault("baseline_total",0)
                county_info.setdefault("total",0)
                county_info.setdefault("baseline_margin",0)
                county_info.setdefault("margin",0)
                county_info["baseline_total"] += row["baseline_total"]
                county_info["total"] += row["total"]
                county_info["baseline_margin"] += (row["baseline_rep"] - row["baseline_dem"])
                county_info["margin"] += (row["rep"] - row["dem"])

                state_info = state_cnts.setdefault(category,{})
                state_info.setdefault("baseline_total",0)
                state_info.setdefault("total",0)
                state_info.setdefault("baseline_margin",0)
                state_info.setdefault("margin",0)
                state_info["baseline_total"] += row["baseline_total"]
                state_info["total"] += row["total"]
                state_info["baseline_margin"] += (row["baseline_rep"] - row["baseline_dem"])
                state_info["margin"] += (row["rep"] - row["dem"])
    return county_cnts, state_cnts

def compute_features(info, early_voting, county_cnts, state_cnts, county_stats, rep_dem_share):
    #compute basic features
    for county in info:
        for precinct in info[county]:
            for category in info[county][precinct]:
                row = info[county][precinct][category]
                row["precinct"] = precinct
                row["county"] = county
                row["category"] = category

                #early vote data + primary info
                early_county = early_voting.get(row["county"],{}).get("votes",{})
                early = early_county.get(row["precinct"],{}).get("total",{})
                early_rep = early_county.get(row["precinct"],{}).get("rep_primary",{})
                early_dem = early_county.get(row["precinct"],{}).get("dem_primary",{})
                row["primary_rep"] = int(early_rep.get(category,0))
                row["primary_dem"] = int(early_dem.get(category,0))
                row["primary_margin"] = row["primary_rep"] - row["primary_dem"]
                row["primary_total"] = row["primary_rep"] + row["primary_dem"]
                row["primary_margin_frac"] = (row["primary_margin"]+1e-6) / (row["primary_total"]+1e-6)

                #pulling the below from simple regression results on nov 3 election
                #margin_frac ~ primary_margin_frac
                #run in match_early_with_primary.py
                if category == "Absentee by Mail Votes":
                    row["primary_margin_pred"] = -0.0538 + 0.862 * row["primary_margin_frac"]
                elif category == "Advanced Voting Votes":
                    row["primary_margin_pred"] = 0.032 + 0.884 * row["primary_margin_frac"]
                row["early_total"] = early.get(category,"")

                #margin = (rep - dem)
                row["baseline_margin"] = int(row["baseline_rep"]) - int(row["baseline_dem"])
                row["margin"] = int(row["rep"]) - int(row["dem"])
                row["margin_frac"] = (row["margin"] + 1e-6) / (row["total"] + 1e-6)

                #compute a ratio of county turnout versus state turnout ("ratio")
                #also difference between county margin frac versus state margin frac ("diff")
                #ratio helped for predicting turnout but diff didn't help with the margin, not sure why
                PSEUDOCOUNTS = 10

                if state_cnts.get(category,None) and (state_cnts[category]["total"] > row["total"]):
                    state_total = state_cnts[category]["total"]
                    state_baseline_total = state_cnts[category]["baseline_total"]
                    state_margin = state_cnts[category]["margin"]
                    state_baseline_margin = state_cnts[category]["baseline_margin"]
                    #exclude results from the precinct itself because this is used in a regression
                    if row["total"]: #TODO: better measure of completeness
                        state_total -= row["total"]
                        state_baseline_total -= row["baseline_total"]
                        state_margin -= row["margin"]
                        state_baseline_margin -= row["baseline_margin"]
                    state_ratio = (state_total + PSEUDOCOUNTS) / (state_baseline_total + PSEUDOCOUNTS)
                    state_margin_frac = (state_margin + PSEUDOCOUNTS) / (state_total + PSEUDOCOUNTS)
                    state_baseline_margin_frac = (state_baseline_margin + PSEUDOCOUNTS) / (state_baseline_total + PSEUDOCOUNTS)
                    state_diff = state_margin_frac - state_baseline_margin_frac
                if county_cnts.get(county,{}).get(category,None) and (county_cnts[county][category]["total"] > row["total"]):
                    county_total = county_cnts[county][category]["total"]
                    county_baseline_total  = county_cnts[county][category]["baseline_total"]
                    county_margin = county_cnts[county][category]["margin"]
                    county_baseline_margin = county_cnts[county][category]["baseline_margin"]
                    #exclude results from the precinct itself because this is used in a regression
                    if row["total"]: #TODO: better measure of completeness
                        county_total -= row["total"]
                        county_baseline_total -= row["baseline_total"]
                        county_margin -= row["margin"]
                        county_baseline_margin -= row["baseline_margin"]
                    county_ratio = (county_total + PSEUDOCOUNTS) / (county_baseline_total + PSEUDOCOUNTS)
                    county_margin_frac = (county_margin + PSEUDOCOUNTS) / (county_total + PSEUDOCOUNTS)
                    county_baseline_margin_frac = (county_baseline_margin + PSEUDOCOUNTS) / (county_baseline_total + PSEUDOCOUNTS)
                    county_diff = county_margin_frac - county_baseline_margin_frac
                    diff = county_diff - state_diff
                    ratio = county_ratio / state_ratio
                else:
                    diff = 0
                    ratio = 1

                #demographics of the county, roughly rescaled to make the range 0-1
                row["edu"] = float(county_stats[row["county"]]["edu"]) / 100
                row["white_pct"] = float(county_stats[row["county"]]["white_pct"])
                row["density"] = float(county_stats[row["county"]]["density"]) / 10

                #predictors
                #adjust for third parties presence in the baseline compared to this election
                row["baseline_two_party_frac"] = (int(row["baseline_rep"]) + int(row["baseline_dem"])+1e-6) / (int(row["baseline_total"])+1e-6)
                two_party_adj_ratio = 1 if rep_dem_share is None else rep_dem_share / row["baseline_two_party_frac"]

                #for predicting margin
                row["baseline_margin_frac"] = two_party_adj_ratio * (row["baseline_margin"] + 1e-6) / (row["baseline_total"] + 1e-6)
                #row["est_margin_frac"] = diff + row["baseline_margin_frac"] #this variable didn't help predictions much
                row["baseline_margin_frac_abs"] = abs(row["baseline_margin_frac"])
                row["baseline_margin_frac_sq"] = abs(row["baseline_margin_frac"]) * row["baseline_margin_frac"]

                #for predicting turnout
                if row["early_total"]:
                    row["est_total"] = row["early_total"]
                    row["est_margin"] = row["early_total"] * row["baseline_margin_frac"]
                else:
                    row["est_total"] = row["baseline_total"] * ratio
                    row["est_margin"] = row["baseline_margin"] * ratio * two_party_adj_ratio

                row["bmf_X_est"] = row["baseline_margin_frac"] * row["est_total"]
                row["edu_X_est"] = row["edu"] * row["est_total"]
                row["white_X_est"] = row["white_pct"] * row["est_total"]
                row["density_X_est"] = row["density"] * row["est_total"]

                yield row

def get_margin_model_info(category):
    early_categories = ['Absentee by Mail Votes','Advanced Voting Votes']
    if category in early_categories:
        #primary vs baseline 25/75 roughly pulled from what would've been reasonable for jan 5 election
        margin_mdl_fields = ["primary_margin_pred","baseline_margin_frac","baseline_margin_frac_abs","white_pct","edu","density"]
        margin_mdl_priors = [0.25,0.75,0,0,0,0]
    else:
        margin_mdl_fields = ["baseline_margin_frac","baseline_margin_frac_sq","baseline_margin_frac_abs","white_pct","edu","density"]
        margin_mdl_priors = [1,0,0,0,0,0]
    return margin_mdl_fields, margin_mdl_priors

def fit_predict_margin_model(category, training, pred):
    #first print results of a simple model meant to be easy to interpret
    simple_margin_mdl = StandardLasso(alpha = 25, prior=[1], intercept_prior=0)
    simple_margin_mdl.fit(training[["baseline_margin_frac"]], training["margin_frac"], sample_weight=training["baseline_total"])
    print("---")
    print("simple margin model:")
    for field, coeff in zip(["intercept", "baseline_margin_frac"], simple_margin_mdl.coeffs()):
        print(f"{field}: {coeff}")
    print("score: " + str(simple_margin_mdl.score(training[["baseline_margin_frac"]], training["margin_frac"], sample_weight=training["baseline_total"])))

    #full model:
    margin_mdl_fields, margin_mdl_priors = get_margin_model_info(category)
    margin_mdl = StandardLasso(alpha = 25, prior=margin_mdl_priors, intercept_prior=0, demean_cols=["baseline_margin_frac_abs","white_pct","edu","density"])
    margin_mdl.fit(training[margin_mdl_fields], training["margin_frac"], sample_weight=training["baseline_total"])

    print("---")
    print("full margin model:")
    for field, coeff in zip(["intercept"] + margin_mdl_fields, margin_mdl.coeffs()):
        print(f"{field}: {coeff}")
    print("score: " + str(margin_mdl.score(training[margin_mdl_fields], training["margin_frac"], sample_weight=training["baseline_total"])))

    return margin_mdl.predict(pred[margin_mdl_fields])

def get_turnout_model_info(category, election_day_ratio = None):
    if election_day_ratio and category == "Election Day Votes":
        cnt_mdl_fields = ["est_total","est_margin","bmf_X_est","white_X_est","edu_X_est","density_X_est"]
        cnt_mdl_priors = [election_day_ratio,0,0,0,0,0]
    else:
        cnt_mdl_fields = ["est_total","est_margin","bmf_X_est","white_X_est","edu_X_est","density_X_est"]
        cnt_mdl_priors = [1,0,0,0,0,0]
    return cnt_mdl_fields, cnt_mdl_priors

def fit_predict_turnout_model(category, training, pred, election_day_ratio = None):
    #first print results of a simple model meant to be easy to interpret
    simple_turnout_mdl = StandardLasso(alpha = 25, prior=[1], fit_intercept=False)
    early_categories = ['Absentee by Mail Votes','Advanced Voting Votes']
    simple_turnout_mdl.fit(training[["est_total"]], training["total"])
    print("---")
    print("simple turnout model:")
    for field, coeff in zip(["est_total"], simple_turnout_mdl.coeffs()):
        print(f"{field}: {coeff}")
    print("score: " + str(simple_turnout_mdl.score(training[["est_total"]], training["total"])))

    #full model
    cnt_mdl_fields, cnt_mdl_priors = get_turnout_model_info(category, election_day_ratio)
    cnt_mdl = StandardLasso(alpha = 25, fit_intercept=False, prior = cnt_mdl_priors)
    cnt_mdl.fit(training[cnt_mdl_fields], training["total"])
    print("---")
    print("full cnt model:")
    for field, coeff in zip(cnt_mdl_fields, cnt_mdl.coeffs()):
        print(f"{field}: {coeff}")
    print("score: " + str(cnt_mdl.score(training[cnt_mdl_fields], training["total"])))

    return cnt_mdl.predict(pred[cnt_mdl_fields])


def generate_predictions(df, election_day_ratio=None):
    all_categories = set(df['category'].tolist())

    #generate predictions
    category_predictions = {}
    for category in all_categories:
        print("---")
        print(f"predicting {category}")
        train_subset = df[(df['category'] == category) & (df['total'] > 0)]
        predict_subset = df[df['category'] == category].copy()

        #NOTE: at first tried fitting just the absolute margin instead of the percent margin
        #which is appealing because it's the exact value you want to know about the election.
        #However, it sort of combines predicting turnout with predicting percent margin
        #and because there's no clear way to know when precincts are done reporting
        #you end up with a lot of incomplete precincts included in the regression
        #which dampens the margin projection.
        #now thinking it's better to project margin frac and turnout separately
        #for those precincts that are partially complete but whose turnout we know

        if len(train_subset) == 0 or category == "Provisional Votes": #TODO: try to regress and fall back on priors
            #TODO: what if baseline_margin_frac is empty as well?
            category_predictions.setdefault(category,pd.DataFrame())

            #baseline margin
            margin_mdl_fields, margin_mdl_priors = get_margin_model_info(category)
            category_predictions[category]["pred_margin_frac"] = predict_subset.apply(lambda x: 0, axis=1) #initialize with 0
            for f, prior in zip(margin_mdl_fields, margin_mdl_priors):
                category_predictions[category]["pred_margin_frac"] += predict_subset[f] * prior

            #baseline turnout
            cnt_mdl_fields, cnt_mdl_priors = get_turnout_model_info(category, election_day_ratio)
            category_predictions[category]["pred_total"] = predict_subset.apply(lambda x: 0, axis=1) #initialize with 0
            for f, prior in zip(cnt_mdl_fields, cnt_mdl_priors):
                category_predictions[category]["pred_total"] += predict_subset[f] * prior
        else:
            predict_subset["pred_margin_frac"] = fit_predict_margin_model(category, train_subset, predict_subset)
            #margin_frac must be between -1 and 1
            #TODO: change to logistic?
            predict_subset["pred_margin_frac"] = predict_subset["pred_margin_frac"].clip(-1,1)

            predict_subset["pred_total"] = fit_predict_turnout_model(category, train_subset, predict_subset, election_day_ratio)
            predict_subset["pred_total"] = predict_subset.apply(lambda x: round(max(x["total"],x["pred_total"])), axis=1) #TODO: is this needed?

            category_predictions[category] = predict_subset[["pred_margin_frac","pred_total"]]

    all_predictions = pd.concat([category_predictions[cat] for cat in all_categories]).sort_index()

    df = df.merge(all_predictions,how="left",left_index=True,right_index=True)
    return df

def process(info, early_voting, projection_constants):
    county_cnts, total_cnts = compute_cnts(info)
    county_stats = load_county_demographics()

    rows = compute_features(info, early_voting, county_cnts, total_cnts, county_stats, projection_constants["rep_dem_share"])
    df = pd.DataFrame(rows)

    #TODO: how to handle partially complete precincts? should be able to make a better prediction for them
    df = generate_predictions(df, projection_constants["election_day_ratio"])

    #generate projections
    def gen_proj(r):
        pred_frac = (r["total"]+1e-6)/(r["pred_total"]+1e-6)

        if r["early_total"]:
            #TODO: allow for vote totals that exceed the early_total projection
            #eg in the jan 5 election there were a few percent more in-person early voting
            #than expected, while mail-in votes were about as expected (as measured on jan 23)?
            proj_total = max(r["early_total"],r["total"])
        elif r["category"] == "Election Day Votes" and pred_frac > 0.5:
            #99% of precincts go straight from 0 to fully counted for election day votes
            #so it's a safe bet every vote has been counted
            proj_total = r["total"]
        elif pred_frac > 0.5:
            #shouldn't get here, as mail-in and early votes are covered by r["early_total"]
            #precincts count in multiple steps 20% of the time for early votes and 50% of the time for mail-ins
            proj_total = r["total"]
        else:
            proj_total = max(r["pred_total"],r["total"])

        proj_frac = (r["total"]+1e-6)/(proj_total+1e-6)

        margin = r["rep"] - r["dem"]
        proj_margin = margin + r["pred_margin_frac"] * (proj_total - r["total"])
        proj_rep = round((proj_total + proj_margin) / 2)
        proj_dem = round((proj_total - proj_margin) / 2)

        return pd.Series([proj_total, proj_rep, proj_dem])

    df[["proj_total","proj_rep","proj_dem"]] = df.apply(gen_proj,axis=1)
    df["outs_total"] = df["proj_total"] - df["total"]
    df["outs_rep"] = df["proj_rep"] - df["rep"]
    df["outs_dem"] = df["proj_dem"] - df["dem"]

    #aggregate to various levels
    export_fields = ["rep","dem","total","outs_rep","outs_dem","outs_total","proj_rep","proj_dem","proj_total"]
    df["county_precinct"] = df.apply(lambda x: x["county"] + "|" + x["precinct"], axis=1)
    precinct_data = dataframe_to_nested_dict(group_by_category_with_total_and_sum(df,['county_precinct'],export_fields)) #TODO: change html to read nested dictionary instead of this wonky county|precinct format
    county_data = dataframe_to_nested_dict(group_by_category_with_total_and_sum(df,['county'],export_fields))
    state_data = dataframe_to_nested_dict(group_by_category_with_total_and_sum(df,[],export_fields))
    return {"precinct": precinct_data, "county": county_data, "state": state_data, "time": datetime.datetime.now().strftime("%-I:%M %p ET, %B %-d, %Y")}

if __name__ ==  "__main__":
    projection_constants = {
        "rep_dem_share": 1, #assuming everyone in the runoff will vote for one of the two candidates, website said no write-ins allowed
        "election_day_ratio": 1.25, #early voting is at 0.77 as of Jan 4 but maybe it would have been higher if not for the holidays, Nate Cohn suggesting 0.81 here: https://twitter.com/Nate_Cohn/status/1345766439135948801
        "election_day_mail_count": 0.83 * 42718, #spitball estimate of the same-day mailin vote to be received on election day as 83% of that received for the general election -- rough extrapolation from looking at the amount coming in each day in early_voting_trends.py
    }

    MEMOIZE=True
    if MEMOIZE:
        if os.path.exists("/tmp/early_voting.pkl"):
            early_voting = pickle.load(open("/tmp/early_voting.pkl","rb")) #TODO: remove, this was just to speed up testing
        else:
            primary_data = get_primary_voting()
            early_voting = read_early_voting_data(35211, "01/05/2021", projection_constants, primary_data = primary_data)
            pickle.dump(early_voting,open("/tmp/early_voting.pkl","wb"))
    else:
        primary_data = get_primary_voting()
        early_voting = read_early_voting_data(35211, "01/05/2021", projection_constants, primary_data = primary_data)

    #generate a smaller dict with county and statewide info and a larger one with all precinct data
    pred = {}
    precinct_pred = {}

    contests = [("perdue", "perdue"), ("perdue", "loeffler")]
    for [baseline, live] in contests:
        print("---")
        print("---")
        print("---")
        print(f"Processing {live} election")

        info = {}

        nov_file = "/tmp/election_results_nov_3.csv"
        jan_file = "election_results_jan_5.csv"

        #load live election data
        load_election(info, nov_file, baseline, "baseline_")

        TESTING = True

        if TESTING:
            load_election(info, jan_file, live, "", "2021-01-05 22:00:00") #test by loading election data up through a certain time and see how the predictions look
            #load_election(info, nov_file, "perdue", "") #test by loading the november data (used before the january data existed)
        else:
            load_election(info, jan_file, live, "")

        data = process(info, early_voting, projection_constants)

        pred.setdefault(live, {})
        pred[live]["county"] = data["county"]
        pred[live]["state"] = data["state"]
        pred["time"] = data["time"]
        precinct_pred[live] = data["precinct"]

    with open("pred.json","w") as f_out:
        f_out.write(json.dumps(pred,cls=NpEncoder));
    with open("precinct-pred.json","w") as f_out:
        f_out.write(json.dumps(precinct_pred,cls=NpEncoder));
