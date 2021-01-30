import glob
import csv
import sys
import os
sys.path.append(os.path.dirname(__file__)) #allow import from current directory
from predict import get_primary_voting, read_early_voting_data
from lasso import StandardLasso
import pickle
import pandas as pd

#following Nate Cohn's lead -- try to regress early voting election results against
#information about which of those early voters voted in each party primary

if __name__ == "__main__":
    from predict_new import load_election
    primary_data = get_primary_voting()

    projection_constants = {}
    # early_voting = read_early_voting_data(35209, "11/03/2020", projection_constants, primary_data = primary_data)
    # pickle.dump(early_voting,open("/tmp/early_voting_nov_3.pkl","wb"))
    # raise
    early_voting = pickle.load(open("/tmp/early_voting_nov_3.pkl","rb")) #TODO: remove, this was just to speed up testing

    info = {}
    nov_file = "/tmp/election_results_nov_3.csv"
    load_election(info, nov_file, "perdue", "")

    early_categories = ['Absentee by Mail Votes','Advanced Voting Votes']

    all_rows = []
    for county in info:
        for precinct in info[county]:
            for category in info[county][precinct]:
                if category not in early_categories: continue
                row = info[county][precinct][category]
                row["precinct"] = precinct
                row["county"] = county
                row["category"] = category


                early_county = early_voting.get(row["county"],{}).get("votes",{})
                early = early_county.get(row["precinct"],{}).get("total",{})
                early_rep = early_county.get(row["precinct"],{}).get("rep_primary",{})
                early_dem = early_county.get(row["precinct"],{}).get("dem_primary",{})
                row["margin"] = int(row["rep"]) - int(row["dem"])
                row["margin_frac"] = (row["margin"] + 1e-6) / (row["total"] + 1e-6)
                row["primary_rep"] = int(early_rep.get(category,0))
                row["primary_dem"] = int(early_dem.get(category,0))
                row["primary_margin"] = row["primary_rep"] - row["primary_dem"]
                row["primary_total"] = row["primary_rep"] + row["primary_dem"]
                row["primary_margin_frac"] = (row["primary_margin"]+1e-6) / (row["primary_total"]+1e-6)
                all_rows.append(row)

    df = pd.DataFrame(all_rows)

    all_categories = set(df['category'].tolist())

    for category in all_categories:
        print(f"predicting {category}")
        train_subset = df[(df['category'] == category) & (df['total'] > 0)]
        margin_mdl_fields = ["primary_margin_frac"]
        margin_mdl_priors = [1]
        margin_mdl = StandardLasso(alpha = 10, prior=margin_mdl_priors, intercept_prior=0)
        margin_mdl.fit(train_subset[margin_mdl_fields], train_subset["margin_frac"], sample_weight=train_subset["total"])
        print("margin model:")
        print(margin_mdl.coeffs())
        print("score")
        print(margin_mdl.score(train_subset[margin_mdl_fields], train_subset["margin_frac"], sample_weight=train_subset["total"]))
