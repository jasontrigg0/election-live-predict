import csv
import requests
import itertools
import random
import json

#1) download minutely
#2) update predictions
#3) push to github

#where to scrape:
#NYT
#Georgia

#read existing data along with the general election results to predict who's winning

#are mailin ballots processed uniformly? are other ballots processed uniformly?

#download precinct map here? https://openprecincts.org/ga/
#looks like it's 30mb, could be better to stick with counties

#what's the vote breakdown among completed precincts?

#TODO: add county FIPS??
#TODO: predict from a statewide estimate of remaining votes
#TODO: predict from a county-by-county estimate of remaining votes

#TODO: complete nov 3 absentee file matching -- maybe 70% done
#TODO: check how the nov 3 absentee file compared to the real turnout, also how much came in on the last day? Also how much is the absentee file adjusted later?

#TODO: ui!

#High level:
#should have a good idea of early voting and mail-in turnout (a lot of precincts are within a few votes)
# - what's the day-of turnout?
# - how are each of the various categories voting compared to the last election?

#data slices to compute:
#(precinct, category) live turnout
#(precinct, category) live turnout
#(precinct, category) live margin pct
#(precinct, category) live margin pct

#(precinct, category) baseline turnout
#(precinct, category) baseline turnout
#(precinct, category) baseline margin pct
#(precinct, category) baseline margin pct

#(category) category_projections (computed internally)
#(precinct, category) completion_estimate (computed internally)
#(precinct, category) completion_estimate (computed internally)

def election_day_complete(results):
    #TODO: adjust as necessary -- one possibility is that this is returned by the georgia website when the precinct is marked as complete. Another possibility is just watching for when the precinct stops adding same day votes while continuing to count other votes
    pass

def election_day_turnout(precinct_data, precinct_completion, baseline_precinct_data):
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
    return {
        "ratio_to_baseline": (real_sum / baseline_sum),
        "total": (real_sum / baseline_sum) * baseline_total
    }


def gen_completion_estimates(precinct_data, precinct_completion, baseline_precinct_data, election_day_ratio):
    #compute a guess from 0% to 100% of the completion of a precinct (by category)
    output = {}
    for precinct in precinct_data:
        output.setdefault(precinct,{})
        for category in precinct_data[precinct]:
            data = precinct_data[precinct][category]
            baseline_turnout = baseline_precinct_data[precinct][category]["total"]
            real_turnout = data["total"]
            if category == "Election Day Votes":
                if precinct_completion[precinct]:
                    data["proj_total"] = data["total"]
                else:
                    data["proj_total"] = (baseline_turnout * election_day_ratio)
            else:
                data["proj_total"] = baseline_turnout

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
    category_margin_delta_sum = {} #weighted total of how precincts are performing compared to baseline election
    category_weight_sum = {}

    #compute the category margin in the baseline
    category_margin_baseline = {} #weighted total of margins
    category_baseline_weight_sum = {}
    for precinct in precinct_data:
        for category in precinct_data[precinct]:
            baseline_data = baseline_precinct_data[precinct][category]
            data = precinct_data[precinct][category]
            category_margin_baseline[category] = category_margin_baseline.get(category,0) + baseline_data["margin_pct"] * baseline_data["total"]
            category_baseline_weight_sum[category] = category_baseline_weight_sum.get(category,0) + baseline_data["total"]

            baseline_margin_pct = baseline_data["margin_pct"]
            margin_pct = data["margin_pct"]
            margin_delta = margin_pct - baseline_margin_pct
            completion_pct = data["total"] / (data["proj_total"] + 1e-6)
            wt = weight_functions["linear"](completion_pct) * data["total"]

            category_margin_delta_sum[category] = category_margin_delta_sum.get(category,0) + (margin_delta * wt)
            category_weight_sum[category] = category_weight_sum.get(category,0) + wt
    output = {}
    for c in category_margin_delta_sum:
        print("testing")
        print(c)
        baseline_margin = category_margin_baseline[c] / category_baseline_weight_sum[c]
        print(baseline_margin)
        margin_delta = category_margin_delta_sum[c] / category_weight_sum[c]
        print(margin_delta)
        output[c] = baseline_margin + margin_delta
    return output


def gen_precinct_projections(precinct_data, baseline_precinct_data, category_projections, election_day_ratio):
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

            # if category == "Election Day Votes":
            #     pass
            completion_pct = live["total"] / (live["proj_total"] + 1e-6)
            margin_pct = live["margin_pct"]

            baseline_margin_pct = baseline["margin_pct"]
            proj_margin_pct = baseline_margin_pct + category_projections[category]
            wt = weight_functions["linear"](completion_pct)

            outstanding_margin_pct = wt * margin_pct + (1 - wt) * proj_margin_pct
            remaining_vote = live["proj_total"] - live["total"]

            live["outs_rep"] = (0.5 + outstanding_margin_pct / 2) * remaining_vote * live["rep_dem_share"]
            live["outs_dem"] = (0.5 - outstanding_margin_pct / 2) * remaining_vote * live["rep_dem_share"]
            live["outs_total"] = remaining_vote
            live["proj_rep"] = live["rep"] + live["outs_rep"]
            live["proj_dem"] = live["dem"] + live["outs_dem"]
            live["proj_total"] = live["total"] + live["outs_total"]

def read_election(filename, generate_test_data = False):
    #return election_data:
    #contest -> county -> "votes" -> precinct -> candidate -> category -> votes
    election_data = {}
    reader = csv.DictReader(open(filename))
    if generate_test_data:
        margin = 0 #-0.02 #simulate republicans getting 2% more margin
        frac_complete = 1 #random.random()
        print("frac_complete", frac_complete)
    for row in reader:
        contest = election_data.setdefault(row["contest"],{})
        county = contest.setdefault(row["county"].strip(),{})
        county_votes = county.setdefault("votes",{})
        precinct = county_votes.setdefault(row["precinct"].strip(),{})
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

def read_early_voting_data(id_):
    #returns early_voting:
    #county_name -> "votes" -> precinct -> "total" -> category -> cnt
    precinct_mapping = load_precinct_mapping() #map from absentee file to early voting
    reader = csv.DictReader(open(f"/home/jason/Downloads/{id_}/STATEWIDE.csv",encoding = "ISO-8859-1"))
    early_voting = {}
    unavailable = set()
    for row in reader:
        if row["Ballot Status"] == "A": #think this means accepted? there's also ["C","S"] both of which I don't know, and "R" = rejected?
            county_name = " ".join([x.capitalize() for x in row["County"].split()]).replace(" ","_")
            county_name = {"Dekalb":"DeKalb", "Mcduffie":"McDuffie", "Mcintosh":"McIntosh"}.get(county_name,county_name)
            county = early_voting.setdefault(county_name,{})
            votes =  county.setdefault("votes",{})
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
            elif row["Ballot Style"] == "IN PERSON":
                totals["Advanced Voting Votes"] += 1
    return early_voting

def load_precinct_mapping():
    data = requests.get("https://www2.census.gov/geo/docs/reference/codes/files/st13_ga_vtd.txt").text
    all_rows = [x.strip() for x in data.split("\n") if x.strip()]
    mapping = {}
    for row in all_rows:
        county = row.split("|")[3].replace("County","").strip().upper()
        precinct_short = row.split("|")[4].strip()
        precinct_full = row.split("|")[5].split("-")[1].replace("Voting District","").strip()
        precinct_full = " ".join([x.capitalize() if x.isalpha() else x for x in precinct_full.split(" ")])
        mapping[(county,precinct_short)] = precinct_full

    mapping[('APPLING', '3A1')] = '3A1'
    mapping[('BERRIEN', '11062')] = 'West Berrien'
    mapping[('BARROW', '01')] = '01 Bethlehem Community Center'
    mapping[('BARROW', '02')] = '02 Bethlehem Church - 211'
    mapping[('BARROW', '03')] = '03 Hmong New Hope Alliance Church'
    mapping[('BARROW', '04')] = '04 Covenant Life Sanctuary'
    mapping[('BARROW', '05')] = '05 Fire Station 1 (Statham)'
    mapping[('BARROW', '08')] = '08 First Baptist Church Winder'
    mapping[('BARROW', '13')] = '13 Winder Community Center'
    mapping[('BARROW', '16')] = '16 The Church at Winder'
    mapping[('BARTOW', '05')] = 'Hamilton Crossing' #another name for Beavers Drive? from googling
    mapping[('BARTOW', '18')] = 'Woodland' #instead of Woodland High
    mapping[('BURKE', '0016')] = 'Blakeney'
    mapping[('BRANTLEY', '1')] = 'Hoboken'
    mapping[('BRANTLEY', '2')] = 'Nahunta'
    mapping[('BROOKS', '1230')] = 'Barwick'
    mapping[('BROOKS','1712')] = 'Pavo'
    #BRYAN county changed substantially since the doc was put together:
    #https://www.bryancountyga.org/home/showpublisheddocument?id=6812
    mapping[('BRYAN','1')] = 'Pembroke'
    mapping[('BRYAN','2')] = 'Ellabell'
    mapping[('BRYAN','3')] = 'Black Creek'
    mapping[('BRYAN','4')] = 'Ways Station'
    mapping[('BRYAN','5')] = 'RH Rec Complex'
    mapping[('BRYAN','6')] = 'JF Gregory Park'
    mapping[('BRYAN','7')] = 'Keller'
    mapping[('BRYAN','8')] = 'Hwy 144 East'
    mapping[('BRYAN','9')] = 'Public Safety Complex'
    mapping[('BRYAN','10')] = 'Danielsiding'
    mapping[('BURKE','0010')] = 'Scotts Crossroads'
    mapping[('BURKE','0012')] = 'St Clair'
    mapping[('BUTTS','BCAB')] = 'Butts County Admin Bldg'
    mapping[('CALHOUN','4')] = 'Edison-Arlington'
    mapping[('CAMDEN','09')] = 'East Kingsland'
    mapping[('CAMDEN','10')] = 'North St Marys'
    mapping[('CAMDEN','11')] = 'St Marys'
    mapping[('CAMDEN','12')] = 'South St Marys'
    mapping[('CAMDEN','14')] = 'West St Marys'
    mapping[('CANDLER','CAND')] = 'Jack Strickland Comm Center'
    mapping[('CARROLL','714BN')] = 'Burson Center'
    mapping[('CARROLL','714A2')] = 'Lakeshore Rec Center'
    mapping[('CARROLL','714A4')] = 'University of W GA'
    mapping[('CARROLL','714A5')] = 'County Admin Bldg'
    mapping[('CATOOSA','WOOD')] = 'Woodstation'
    mapping[('CATOOSA','FT O')] = 'Ft Oglethorpe'
    mapping[('CHARLTON','3B')] = 'GA Bend'
    mapping[('CHARLTON','3S')] = 'St George'
    mapping[('CHARLTON','4H')] = 'Homeland'
    mapping[('CHARLTON','4A')] = 'American Legion'
    mapping[('CHARLTON','5F')] = 'Folkston Fire'
    mapping[('CHATHAM','8-07C')] = '8-07C Woodville-Tompkins TI'
    mapping[('CHATHAM','3-01C')] = '3-01C Old Courthouse'
    mapping[('CHATHAM','5-02C')] = '5-02C Senior Citizens Center'
    mapping[('CHATHAM','8-01C')] = '8-01C Civic Center'
    mapping[('CHATHAM','2-06C')] = '2-06C Eli Whitney Complex'
    mapping[('CHATHAM','8-13C')] = '8-13C Savannah Christian'
    mapping[('CHATHAM','3-09C')] = '3-09C Cokesbury Methodist'
    mapping[('CHATHAM','8-03C')] = '8-03C Silk Hope Baptist Church'
    mapping[('CHATHAM','1-09C')] = '1-09C Immanuel Baptist Church'
    mapping[('CHATHAM','8-06C')] = '8-06C Tompkins Rec Center'
    mapping[('CHATHAM','5-03C')] = '5-03C Butler Presbyterian Church'
    mapping[('CHATHAM','7-03C')] = '7-03C PB Edwards Gym'
    mapping[('CHATHAM','8-09C')] = '8-09C Moses Jackson Center'
    mapping[('CHATHAM','8-05C')] = '8-05C W Broad St YMCA'
    mapping[('CHATHAM','5-01C')] = '5-01C Bartlett Middle School'
    mapping[('CHATHAM','3-03C')] = '3-03C Savannah High School'
    mapping[('CHATHAM','3-15C')] = '3-15C Eli Whitney Complex'
    mapping[('CHATHAM','3-04C')] = '3-04C First African Baptist Church'
    mapping[('CHATHAM','8-12C')] = '8-12C Beach High School'
    mapping[('CHATHAM','1-10C')] = '1-10C St Thomas Episcopal Church'
    mapping[('CHATHAM','8-08C')] = '8-08C Resur of Our Lord Church'
    mapping[('CHATHAM','8-10C')] = '8-10C Carver Heights Comm Ctr'
    mapping[('CHATHAM','4-11C')] = '4-11C Tybee Island School Cafe'
    mapping[('CHATHAM','2-03C')] = '2-03C W W Law Center'
    mapping[('CHATHAM','7-05C')] = '7-05C Woodlawn Baptist Church'
    mapping[('CHATHAM','8-11C')] = '8-11C Butler Elementary'
    mapping[('CHATHAM','8-15C')] = '8-15C Garden City Rec Center'
    mapping[('CHATHAM','3-11C')] = '3-11C Southside Baptist Church'
    mapping[('CHATHAM','2-11C')] = '2-11C Stillwell Towers'
    mapping[('CHATHAM','2-02C')] = '2-02C Blackshear Community Center'
    mapping[('CHATHAM','3-08C')] = '3-08C Jenkins High School'
    mapping[('CHATHAM','7-01C')] = '7-01C Garden City Senior Ctr'
    mapping[('CHATHAM','4-10C')] = '4-10C Guard House Comm Ctr'
    mapping[('CHATHAM','1-13C')] = '1-13C The Sanctuary'
    mapping[('CHATHAM','3-12C')] = '3-12C Thunderbolt Muni Complex'
    mapping[('CHATHAM','6-06C')] = '6-06C The Light Church'
    mapping[('CHATHAM','2-12C')] = '2-12C Williams Court Apts'
    mapping[('CHATHAM','5-07C')] = '5-07C Station 1'
    mapping[('CHATHAM','8-02C')] = '8-02C Hellenic Center'
    mapping[('CHATHAM','1-08C')] = '1-08C Grace United Methodist Church'
    mapping[('CHATHAM','4-14C')] = '4-14C Skidaway Island Baptist'
    mapping[('CHATHAM','5-08C')] = '5-08C Savannah Primitive BC'
    mapping[('CHATHAM','3-05C')] = '3-05C Aldersgate Youth Center'
    mapping[('CHATHAM','4-15C')] = '4-15C Skidaway Island State Park'
    mapping[('CHATHAM','6-01C')] = '6-01C White Bluff Presbyterian'
    mapping[('CHATHAM','3-02C')] = '3-02C Temple Mickve Israel'
    mapping[('CHATHAM','6-05C')] = '6-05C Windsor Hall'
    mapping[('CHATHAM','5-06C')] = '5-06C Seed Church'
    mapping[('CHATHAM','1-14C')] = '1-14C Compassion Christian Church'
    mapping[('CHATHAM','2-05C')] = '2-05C Holy Spirit Lutheran Church'
    mapping[('CHATHAM','6-08C')] = '6-08C Christ Memorial Baptist Church'
    mapping[('CHATHAM','1-12C')] = '1-12C Isle of Hope Baptist'
    mapping[('CHATHAM','1-06C')] = '1-06C Central Church of Christ'
    mapping[('CHATHAM','5-05C')] = '5-05C Liberty City Comm Ctr'
    mapping[('CHATHAM','3-10C')] = '3-10C Bible Baptist Church'
    mapping[('CHATHAM','3-13C')] = '3-13C New Cov 7 Day Adv Ch'
    mapping[('CHATHAM','4-12C')] = '4-12C St Peters Episcopal'
    mapping[('CHATHAM','3-14C')] = '3-14C Oglethorpe Charter Academy'
    mapping[('CHATHAM','1-05C')] = '1-05C JEA Building'
    mapping[('CHATHAM','2-07C')] = '2-07C Christ Community Church'
    mapping[('CHATHAM','7-15C')] = '7-15C Rice Creek School'
    mapping[('CHATHAM','2-04C')] = '2-04C Fellowship of Love Church'
    mapping[('CHATHAM','4-05C')] = '4-05C St Francis Episcopal Church'
    mapping[('CHATHAM','4-13C')] = '4-13C Skidaway Island Pres Church'
    mapping[('CHATHAM','6-02C')] = '6-02C Windsor Forest Baptist'
    mapping[('CHATHAM','7-09C')] = '7-09C Savannah Holy C of G'
    mapping[('CHATHAM','4-04C')] = '4-04C Lighthouse Baptist Church'
    mapping[('CHATHAM','5-10C')] = '5-10C Tatumville Community Center'
    mapping[('CHATHAM','4-06C')] = '4-06C First Baptist of the Island'
    mapping[('CHATHAM','5-11C')] = '5-11C Largo-Tibet Elementary'
    mapping[('CHATHAM','4-02C')] = '4-02C Frank Murray Comm Center'
    mapping[('CHATHAM','7-08C')] = '7-08C Bloomingdale Comm Ctr'
    mapping[('CHATHAM','6-03C')] = '6-03C Crusader Comm Center'
    mapping[('CHATHAM','2-09C')] = '2-09C Salvation Army'
    mapping[('CHATHAM','1-16C')] = '1-16C Ferguson Ave Baptist'
    mapping[('CHATHAM','4-08C')] = '4-08C Wilmington Island Pres Church'
    mapping[('CHATHAM','7-04C')] = '7-04C Lake Shore Comm Ctr'
    mapping[('CHATHAM','6-09C')] = '6-09C Trinity Lutheran Church'
    mapping[('CHATHAM','7-13C')] = '7-13C Southside Fire Trng Ctr'
    mapping[('CHATHAM','4-07C')] = '4-07C Wilmington Island UMC'
    mapping[('CHATHAM','1-01C')] = '1-01C First Presbyterian Church'
    mapping[('CHATHAM','1-17C')] = '1-17C Islands Christian Church'
    mapping[('CHATHAM','8-16C')] = '8-16C Royal Cinemas and IMAX'
    mapping[('CHATHAM','7-06C')] = '7-06C Pooler City Hall'
    mapping[('CHATHAM','7-10C')] = '7-10C Progressive Rec Center'
    mapping[('CHATHAM','7-12C')] = '7-12C Pooler Church'
    mapping[('CHATHAM','6-10C')] = '6-10C Station 3'
    mapping[('CHATHAM','7-07C')] = '7-07C Rothwell Baptist Church'
    mapping[('CHATHAM','7-16C')] = '7-16C Pooler Recreation Center Gymnasium'
    mapping[('CHATHAM','6-11C')] = '6-11C Bamboo Farms'
    mapping[('CHATHAM','7-11C')] = '7-11C Seventh Day Adv Church'
    mapping[('CHATHAM','7-14C')] = '7-14C Coastal Cathedral'
    mapping[('CHATTOOGA','C-968')] = 'Cloudland'
    mapping[('CHATTOOGA','P-870')] = 'Pennville'
    mapping[('CHATTOOGA','927')] = 'Teloga'
    mapping[('CHEROKEE','012')] = 'Woodstock'
    mapping[('CHEROKEE','044')] = 'Neese'
    mapping[('CLARKE','6D')] = '6D'
    mapping[('CLARKE','8B')] = '8B'
    mapping[('CLARKE','4A')] = '4A'
    mapping[('CLARKE','3A')] = '3A'
    mapping[('CLARKE','1A')] = '1A'
    mapping[('CLARKE','1C')] = '1C'
    mapping[('CLARKE','5C')] = '5C'
    mapping[('CLARKE','5D')] = '5D'
    mapping[('CLARKE','2A')] = '2A'
    mapping[('CLARKE','8A')] = '8A'
    mapping[('CLARKE','5B')] = '5B'
    mapping[('CLARKE','1D')] = '1D'
    mapping[('CLARKE','7C')] = '7C'
    mapping[('CLARKE','7A')] = '7A'
    mapping[('CLARKE','6C')] = '6C'
    mapping[('CLARKE','7B')] = '7B'
    mapping[('CLARKE','8C')] = '8C'
    mapping[('CLARKE','1B')] = '1B'
    mapping[('CLARKE','6B')] = '6B'
    mapping[('CLARKE','5A')] = '5A'
    mapping[('CLARKE','3B')] = '3B'
    mapping[('CLARKE','6A')] = '6A'
    mapping[('CLARKE','2B')] = '2B'
    mapping[('CLARKE','4B')] = '4B'
    mapping[('CLAY','3')] = 'Court House'
    mapping[('CLAY','5')] = 'Days Cross Road'
    mapping[('CLAYTON','OAK5')] = 'Oak 5'
    mapping[('CLAYTON','MO11')] = 'Morrow 11'
    mapping[('CLAYTON','MO10')] = 'Morrow 10'
    mapping[('CLAYTON','LJ7')] = 'Lovejoy 7'
    mapping[('CLAYTON','JB19')] = 'Jonesboro 19'
    mapping[('CLAYTON','EW1')] = 'Ellenwood 1'
    mapping[('CLAYTON','LJ6')] = 'Lovejoy 6'
    mapping[('CLAYTON','EW2')] = 'Ellenwood 2'
    mapping[('COBB','MK01')] = 'McCleskey 01'
    mapping[('COBB','MR4A')] = 'Marietta 4A'
    mapping[('COBB','MR3B')] = 'Marietta 3B'
    mapping[('COBB','SN3B')] = 'Smyrna 3B'
    mapping[('COBB','SF01')] = 'Shallowford Falls 01'
    mapping[('COBB','CR01')] = 'Chestnut Ridge 01'
    mapping[('COBB','BF04')] = 'Bells Ferry 04'
    mapping[('COBB','DI02')] = 'Dobbins 02'
    mapping[('COBB','PS2A')] = 'Powders Springs 2A'
    mapping[('COBB','ML01')] = 'McClure 01'
    mapping[('COBB','EA02')] = 'Eastside 02'
    mapping[('COBB','ME01')] = 'McEachern 01'
    mapping[('COBB','PS3A')] = 'Powders Springs 3A'
    mapping[('COBB','PS1A')] = 'Powders Springs 1A'
    mapping[('COBB','HL01')] = 'Harmony-Leland 01'
    mapping[('COLUMBIA','017')] = 'Patriots Park'
    mapping[('COLUMBIA','018')] = 'New Life Church'
    mapping[('COLUMBIA','022')] = 'Harlem Senior Center'
    mapping[('COLUMBIA','024')] = 'Second Mt Moriah Baptist Church'
    mapping[('COLUMBIA','034')] = 'Grove First Baptist Church'
    mapping[('COLUMBIA','075')] = 'Belair Baptist Church'
    mapping[('COLUMBIA','105')] = 'Savannah Rapids Pavilion'
    mapping[('COLUMBIA','108')] = 'Genesis Church'
    mapping[('COLUMBIA','050')] = 'Second Mt Carmel Baptist Church'
    mapping[('COLUMBIA','051')] = 'Damascus Baptist Church'
    mapping[('COLUMBIA','110')] = 'Mtz Col Fire Hdqtr'
    mapping[('COLUMBIA','025')] = 'Bessie Thomas Center'
    mapping[('COLUMBIA','080')] = 'Westside Baptist Church'
    mapping[('COLUMBIA','016')] = 'Woodlawn Baptist Church'
    mapping[('COLUMBIA','040')] = 'Eubank-Blanchard Center'
    mapping[('COLUMBIA','131')] = 'Journey Comm Church'
    mapping[('COLUMBIA','063')] = 'Riverview Church'
    mapping[('COLUMBIA','120')] = 'Gold Cross EMS'
    mapping[('COLUMBIA','125')] = 'Church of Our Savior'
    mapping[('COLUMBIA','031')] = 'Grovetown Methodist Church'
    mapping[('COLUMBIA','136')] = 'Blue Ridge Elementary'
    mapping[('COLUMBIA','107')] = 'Gospel Water Branch'
    mapping[('COLUMBIA','135')] = 'Christ Church Presbyterian'
    mapping[('COLUMBIA','060')] = 'Col Cty Bd of Edu'
    mapping[('COLUMBIA','137')] = 'Christ The King Luth Church'
    mapping[('COLUMBIA','065')] = 'Christ Sanctified'
    mapping[('COLUMBIA','085')] = 'Trinity Baptist Church'
    mapping[('COLUMBIA','020')] = 'Harlem Branch Library'
    mapping[('COLUMBIA','109')] = 'Stevens Creek Church'
    mapping[('COLUMBIA','030')] = 'Liberty Park-Grovetown'
    mapping[('COLUMBIA','010')] = 'Kiokee Baptist Church'
    mapping[('COLUMBIA','033')] = 'Grovetown Public Safety Station 2'
    mapping[('COLUMBIA','015')] = 'Lewis Methodist Church'
    mapping[('COLUMBIA','062')] = 'Parkway Baptist Church'
    mapping[('COOK','EP')] = 'Elm Pine'
    mapping[('COOK','NL')] = 'New Life Baptist Church'
    mapping[('COWETA','27')] = 'Coweta Central Library'
    mapping[('COWETA','18')] = 'Newnan Centre'
    mapping[('DAWSON','01')] = 'West'
    mapping[('DAWSON','03')] = 'East'
    mapping[('DAWSON','02')] = 'Central'
    mapping[('DECATUR','1277')] = 'Mt Pleasant'
    mapping[('DECATUR','514')] = 'BDGE-Fairgrounds'
    mapping[('DECATUR','513')] = 'Bainbridge-Coliseum'
    mapping[('DEKALB','CT')] = 'Covington Hwy'
    mapping[('DEKALB','PE')] = 'Pine Lake'
    mapping[('DEKALB','CC')] = 'Columbia Elem'
    mapping[('DEKALB','LD')] = 'Lithonia'
    mapping[('DEKALB','SQ')] = 'Stone Mtn'
    mapping[('DEKALB','KC')] = 'Kelley Chapel Road'
    mapping[('DEKALB','HI')] = 'Harris-Margaret Harris ED'
    mapping[('DEKALB','CL')] = 'Clifton'
    mapping[('DEKALB','CB')] = 'Canby Lane Elem'
    mapping[('DEKALB','SN')] = 'Shamrock'
    mapping[('DEKALB','GF')] = 'Glenwood Road'
    mapping[('DEKALB','IB')] = 'Indian Creek'
    mapping[('DEKALB','SD')] = 'Stone Mountain Elem'
    mapping[('DEKALB','MP')] = 'McNair Academy'
    mapping[('DEKALB','SI')] = 'Stone Mtn Middle'
    mapping[('DEKALB','PA')] = 'Peachcrest'
    mapping[('DEKALB','LV')] = 'Livsey Elem'
    mapping[('DEKALB','AF')] = 'Austin Drive'
    mapping[('DEKALB','BD')] = 'Briarlake Elem'
    mapping[('DEKALB','CY')] = 'Clarkston Community Center'
    mapping[('DEKALB','KE')] = 'Knollwood'
    mapping[('DEKALB','FK')] = 'Flakes Mill Fire Station'
    mapping[('DEKALB','BL')] = 'Bouldercrest Road'
    mapping[('DEKALB','MG')] = 'Medlock'
    mapping[('DEKALB','MO')] = 'Midway'
    mapping[('DEKALB','SP')] = 'Stone Mtn Champion'
    mapping[('DEKALB','MD')] = 'McNair'
    mapping[('DEKALB','WA')] = 'Wadsworth'
    mapping[('DEKALB','SC')] = 'Shaw-Robert Shaw Elem'
    mapping[('DEKALB','CE')] = 'Chamblee'
    mapping[('DEKALB','CH')] = 'Chesnut Elem'
    mapping[('DEKALB','DD')] = 'Decatur'
    mapping[('DEKALB','WF')] = 'Winnona Park'
    mapping[('DEKALB','AA')] = 'Allgood Elem'
    mapping[('DEKALB','GA')] = 'Glennwood'
    mapping[('DEKALB','MR')] = 'Mathis-Bob Mathis Elem'
    mapping[('DEKALB','PG')] = 'Ponce de Leon'
    mapping[('DEKALB','BC')] = 'Briar Vista Elem'
    mapping[('DEKALB','KG')] = 'Kittredge Elem'
    mapping[('DEKALB','TA')] = 'Terry Mill'
    mapping[('DEKALB','DG')] = 'Dunwoody 2'
    mapping[('DEKALB','ML')] = 'Meadowview'
    mapping[('DEKALB','CK')] = 'Clarkston'
    mapping[('DEKALB','AB')] = 'Ashford Park Elem'
    mapping[('DEKALB','LH')] = 'Lithonia High'
    mapping[('DEKALB','OK')] = 'Oakhurst'
    mapping[('DEKALB','SA')] = 'Sagamore Hills Elem'
    mapping[('DEKALB','WB')] = 'Wesley Chapel Library'
    mapping[('DEKALB','AE')] = 'Avondale'
    mapping[('DEKALB','WJ')] = 'Woodward'
    mapping[('DEKALB','ME')] = 'McLendon'
    mapping[('DEKALB','LE')] = 'Lin-Mary Lin Elem'
    mapping[('DEKALB','GC')] = 'Gresham Road'
    mapping[('DEKALB','CG')] = 'Chapel Hill Elem'
    mapping[('DEKALB','AI')] = 'Avondale High'
    mapping[('DEKALB','RH')] = 'Redan-Trotti Library'
    mapping[('DEKALB','PF')] = 'Pleasantdale Road'
    mapping[('DEKALB','PB')] = 'Peachtree Middle'
    mapping[('DEKALB','MI')] = 'Miller Grove'
    mapping[('DEKALB','AG')] = 'Ashford Dunwoody Rd'
    mapping[('DEKALB','BR')] = 'Burgess Elem'
    mapping[('DEKALB','CZ')] = 'Chamblee 2'
    mapping[('DEKALB','BJ')] = 'Browns Mill Elem'
    mapping[('DEKALB','CD')] = 'Cedar Grove Elem'
    mapping[('DEKALB','SW')] = 'Stonecrest Library'
    mapping[('DEKALB','HH')] = 'Harris-Narvie J Harris Elem'
    mapping[('DEKALB','EA')] = 'East Lake'
    mapping[('DEKALB','CN')] = 'Coan Recreation Center'
    mapping[('DEKALB','CX')] = 'Candler Park'
    mapping[('DEKALB','BB')] = 'Boulevard'
    mapping[('DEKALB','DH')] = 'Druid Hills High'
    mapping[('DEKALB','CU')] = 'Covington'
    mapping[('DEKALB','MF')] = 'McWilliams'
    mapping[('DEKALB','MA')] = 'Miller-Eldridge L Miller Elem'
    mapping[('DEKALB','CJ')] = 'Brockett Elem'
    mapping[('DEKALB','BF')] = 'Clairmont Road'
    mapping[('DEKALB','MB')] = 'Candler-Murphey Candler Elem'
    mapping[('DEKALB','CF')] = 'McNair High'
    mapping[('DEKALB','SR')] = 'Snapfinger Road N'
    mapping[('DEKALB','SS')] = 'Snapfinger Road S'
    mapping[('DEKALB','SY')] = 'Snapfinger Road'
    mapping[('DODGE','CLARK')] = 'Clark'
    mapping[('DODGE','MCCRA')] = 'McCranie'
    mapping[('DODGE','EDDIN')] = 'Eddin'
    mapping[('DOUGHERTY','01')] = 'Palmyra Methodist'
    mapping[('DOUGHERTY','02')] = 'Sherwood Elementary'
    mapping[('DOUGHERTY','03')] = 'Covenant Church'
    mapping[('DOUGHERTY','04')] = 'Greenbriar Church'
    mapping[('DOUGHERTY','07')] = 'Darton College'
    mapping[('DOUGHERTY','08')] = 'Westtown Elementary'
    mapping[('DOUGHERTY','09')] = '2nd Mt Zion Church'
    mapping[('DOUGHERTY','10')] = 'Mt Zion Center'
    mapping[('DOUGHERTY','11')] = 'Alice Coachman Elementary'
    mapping[('DOUGHERTY','12')] = 'Carver Teen Center'
    mapping[('DOUGHERTY','13')] = 'Shiloh Baptist Church'
    mapping[('DOUGHERTY','14')] = 'Litman Cathedral'
    mapping[('DOUGHERTY','15')] = 'Phoebe Healthworks'
    mapping[('DOUGHERTY','16')] = 'Turner Elementary'
    mapping[('DOUGHERTY','17')] = 'Jackson Heights Elementary'
    mapping[('DOUGHERTY','18')] = 'Bill Miller Center'
    mapping[('DOUGHERTY','19')] = 'Radium Middle School'
    mapping[('DOUGHERTY','20')] = 'Putney 1st Baptist Church'
    mapping[('DOUGHERTY','21')] = 'International Studies'
    mapping[('DOUGHERTY','22')] = 'Phoebe East'
    mapping[('DOUGHERTY','24')] = 'Albany Middle School'
    mapping[('DOUGHERTY','23')] = 'Pine Bluff Baptist Church'
    mapping[('DOUGHERTY','25')] = 'Christ Church'
    mapping[('DOUGHERTY','26')] = 'Lamar Reese Elementary'
    mapping[('DOUGHERTY','27')] = '1st Christian Church'
    mapping[('DOUGHERTY','28')] = 'Lovett Hall'
    mapping[('DOUGLAS','1274')] = 'First Baptist Lithia Springs'
    mapping[('DOUGLAS','738')] = 'St Julians Episcopal'
    mapping[('DOUGLAS','740')] = 'First Baptist Douglasville'
    mapping[('DOUGLAS','1270')] = 'Atlanta West Pentecostal'
    mapping[('DOUGLAS','731')] = 'Beulah Baptist Church'
    mapping[('DOUGLAS','739')] = 'Lutheran Church-GS'
    mapping[('DOUGLAS','730')] = 'Old Courthouse'
    mapping[('DOUGLAS','729')] = 'Golden Methodist Church'
    mapping[('DOUGLAS','1275')] = 'Lithia Springs High School'
    mapping[('DOUGLAS','1272')] = 'Prays Mill Gym'
    mapping[('DOUGLAS','1258')] = 'Mirror Lake Elementary'
    mapping[('DOUGLAS','1260')] = 'Dog River Library'
    mapping[('DOUGLAS','736S')] = 'Church At Chapel Hill'
    mapping[('ELBERT','196')] = 'Moss-Ruckersville'
    mapping[('EMANUEL','0052')] = 'Cross-Green'
    mapping[('EVANS','11')] = 'Veterans Community Center'
    mapping[('FAYETTE','16')] = 'McIntosh'
    mapping[('FLOYD','045')] = 'Fosters Mill'
    mapping[('FORSYTH','34')] = '34 Fowler'
    mapping[('FORSYTH','36')] = '36 Nichols'
    mapping[('FORSYTH','05')] = '05 Coal Mountain'
    mapping[('FORSYTH','35')] = '35 Johns Creek'
    mapping[('FORSYTH','03')] = '03 Browns Bridge'
    mapping[('FORSYTH','37')] = '37 Sawnee'
    mapping[('FRANKLIN','2')] = 'West Franklin'
    mapping[('FRANKLIN','7')] = 'Southwest Franklin'
    mapping[('FRANKLIN','3')] = 'Northeast Franklin'
    mapping[('GLYNN','1713')] = 'Sterling Elementary'
    mapping[('GLYNN','2933')] = 'St William Catholic Church'
    mapping[('GLYNN','2943')] = 'First Baptist'
    mapping[('GLYNN','2953')] = 'Jekyll Island'
    mapping[('GLYNN','3723')] = 'SE Baptist Bldg'
    mapping[('GLYNN','5913')] = 'College Place UMC'
    mapping[('GLYNN','5943')] = 'Selden Park'
    mapping[('GLYNN','1943')] = 'Blythe Island Baptist'
    mapping[('GLYNN','1923')] = 'Brookman'
    mapping[('GLYNN','2923')] = 'Hampton River'
    mapping[('GLYNN','5933')] = 'Urbana-Perry Parks'
    mapping[('GLYNN','4933')] = 'The Chapel'
    mapping[('GLYNN','5923')] = 'Howard Coffin Park'
    mapping[('GLYNN','4923')] = 'Northside Baptist'
    mapping[('GLYNN','3733')] = 'Career Academy'
    mapping[('GLYNN','4913')] = 'Ballard'
    mapping[('GLYNN','1933')] = 'Satilla Marshes'
    mapping[('GLYNN','3743')] = 'C B Greer School'
    mapping[('GLYNN','3713')] = 'Sterling Church of God'
    mapping[('GLYNN','2713')] = 'Christian Renewal'
    mapping[('GORDON','849-A')] = 'Gordon County'
    mapping[('GORDON','849-B')] = 'Calhoun City'
    mapping[('GRADY','C04')] = 'Cairo 4th District'
    mapping[('GRADY','C05')] = 'Cairo 5th District'
    mapping[('HABERSHAM','01')] = 'Habersham North'
    mapping[('HABERSHAM','02')] = 'Habersham South'
    mapping[('HABERSHAM','04')] = 'Town of Mt Airy'
    mapping[('HABERSHAM','05')] = 'City of Baldwin'
    mapping[('HABERSHAM','06')] = 'Mud Creek'
    mapping[('HABERSHAM','07')] = 'Amys Creek'
    mapping[('HALL','001')] = 'Wilson'
    mapping[('HALL','002')] = 'Chicopee'
    mapping[('HALL','003')] = 'Oakwood I'
    mapping[('HALL','004')] = 'Oakwood II'
    mapping[('HALL','005')] = 'Flowery Branch I'
    mapping[('HALL','006')] = 'Flowery Branch II'
    mapping[('HALL','007')] = 'Roberts'
    mapping[('HALL','008')] = 'Morgan I'
    mapping[('HALL','009')] = 'Morgan II'
    mapping[('HALL','010')] = 'Candler'
    mapping[('HALL','011')] = 'Tadmore'
    mapping[('HALL','012')] = 'Glade'
    mapping[('HALL','013')] = 'Lula'
    mapping[('HALL','014')] = 'Clermont'
    mapping[('HALL','015')] = 'Quillians'
    mapping[('HALL','016')] = 'Bark Camp'
    mapping[('HALL','017')] = 'Chestatee'
    mapping[('HALL','018')] = 'Fork'
    mapping[('HALL','019')] = 'Whelchel'
    mapping[('HALL','020')] = 'West Whelchel'
    mapping[('HALL','021')] = 'Gainesville I'
    mapping[('HALL','022')] = 'Gainesville II'
    mapping[('HALL','023')] = 'Gainesville III'
    mapping[('HALL','024')] = 'Gainesville IV'
    mapping[('HALL','025')] = 'Gainesville V'
    mapping[('HALL','026')] = 'Gillsville'
    mapping[('HALL','027')] = 'Big Hickory'
    mapping[('HALL','028')] = 'Friendship I'
    mapping[('HALL','029')] = 'Friendship II'
    mapping[('HALL','030')] = 'Friendship III'
    mapping[('HALL','031')] = 'Friendship IV'
    mapping[('HANCOCK','4C')] = 'Sparta 4C'
    mapping[('HANCOCK','3A')] = 'Mayfield Community Center'
    mapping[('HANCOCK','4B')] = 'Second Darrien'
    mapping[('HANCOCK','3C')] = 'Warren Chapel'
    mapping[('HANCOCK','2B')] = 'Second Beulah'
    mapping[('HANCOCK','1B')] = 'St Mark'
    mapping[('HANCOCK','3B')] = 'Power of God'
    mapping[('HANCOCK','4A')] = 'Youth Center'
    mapping[('HANCOCK','2A')] = 'Courthouse'
    mapping[('HANCOCK','1A')] = 'Devereux Fire Station'
    mapping[('HARALSON','11')] = 'Berea-Steadman'
    mapping[('HARRIS','PV')] = 'Pine Mountain Valley'
    mapping[('HARRIS','U9')] = 'Upper 19th'
    mapping[('HARRIS','L9')] = 'Lower 19th'
    mapping[('HEARD','COO')] = 'Cooksville-Corinth'
    mapping[('HENRY','35')] = 'McDonough'
    mapping[('HENRY','58')] = 'Mt Bethel'
    mapping[('HENRY','54')] = 'Stockbridge Central'
    mapping[('HENRY','36')] = 'McMullen'
    mapping[('HENRY','61')] = 'McDonough Central'
    mapping[('HENRY','32')] = 'Mt Carmel'
    mapping[('HENRY','39')] = 'Stockbridge East-West'
    mapping[('HENRY','33')] = 'Red Oak'
    mapping[('HOUSTON','HEFS')] = 'HEFS'
    mapping[('HOUSTON','HAFS')] = 'HAFS'
    mapping[('HOUSTON','NSES')] = 'NSES'
    mapping[('HOUSTON','TWPK')] = 'TWPK'
    mapping[('HOUSTON','TMS')] = 'TMS'
    mapping[('HOUSTON','ANNX')] = 'ANNX'
    mapping[('HOUSTON','HCTC')] = 'HCTC'
    mapping[('HOUSTON','RECR')] = 'RECR'
    mapping[('HOUSTON','CENT')] = 'CENT'
    mapping[('HOUSTON','FMMS')] = 'FMMS'
    mapping[('HOUSTON','BMS')] = 'BMS'
    mapping[('HOUSTON','MCMS')] = 'MCMS'
    mapping[('JACKSON','4000')] = 'South Jackson'
    mapping[('JACKSON','2000')] = 'North Jackson'
    mapping[('JACKSON','3000')] = 'West Jackson'
    mapping[('JACKSON','1000')] = 'Central Jackson'
    mapping[('JASPER','296')] = 'Martin-Burney'
    mapping[('JEFFERSON','0076')] = 'Stapleton Crossroads'
    mapping[('JENKINS','1')] = 'Primary School'
    mapping[('JENKINS','4')] = 'Senior Citizens Center'
    mapping[('LAMAR','1712B')] = 'Senior Citizen Bldg'
    mapping[('LAMAR','1712A')] = 'Chappell Mill V FD'
    mapping[('LAMAR','1711')] = 'Barnesville'
    mapping[('LAURENS','15')] = 'Minter'
    mapping[('LAURENS','22')] = 'LCFS 10 Valambrosia'
    mapping[('LAURENS','16')] = 'Rural Fire Sta 17'
    mapping[('LAURENS','18')] = 'W T Adams Fire Sta 18'
    mapping[('LAURENS','05')] = 'Fire Dept Sta 5'
    mapping[('LAURENS','03')] = 'FBC-FLC'
    mapping[('LEE','1')] = 'Chokee'
    mapping[('LEE','3')] = 'CJC'
    mapping[('LEE','2')] = 'Smithville'
    mapping[('LEE','5')] = 'Friendship Baptist'
    mapping[('LEE','7')] = 'SDA Church'
    mapping[('LEE','8')] = 'Flint Reformed Baptist'
    mapping[('LEE','4')] = 'Leesburg'
    mapping[('LEE','9')] = 'Century Fire Station'
    mapping[('LEE','6')] = 'First Baptist'
    mapping[('LEE','10')] = 'Redbone'
    mapping[('LIBERTY','1')] = 'Riceboro Youth Center'
    mapping[('LIBERTY','12')] = 'Memorial Dr East'
    mapping[('LIBERTY','9')] = 'Hinesville Lodge 271'
    mapping[('LIBERTY','14')] = 'Town of Allenhurst'
    mapping[('LIBERTY','11')] = 'Lewis Frasier School'
    mapping[('LIBERTY','3')] = 'Liberty County Complex'
    mapping[('LINCOLN','1-A')] = 'Midway'
    mapping[('LINCOLN','1-B')] = 'Lincoln Club House'
    mapping[('LINCOLN','3-A')] = 'Faith Temple of Lincoln'
    mapping[('LINCOLN','3-B')] = 'Tabernacle'
    mapping[('LINCOLN','4-A')] = 'Bethany Church'
    mapping[('LINCOLN','4-B')] = 'Martins Crossroad'
    mapping[('LONG','6')] = 'Alma Flournoy'
    mapping[('LONG','7')] = 'Faith Baptist Annex'
    mapping[('LONG','2')] = 'Rye Patch/Oak Dale'
    mapping[('LOWNDES','010')] = 'VSU UC # 2'
    mapping[('LOWNDES','004')] = 'Naylor'
    mapping[('LOWNDES','012')] = 'Northgate Assembly'
    mapping[('LOWNDES','013')] = 'Mt Calvary'
    mapping[('LOWNDES','006')] = 'Mildred'
    mapping[('LOWNDES','009')] = 'S Lowndes'
    mapping[('LOWNDES','007')] = 'Clyattville'
    mapping[('LOWNDES','008')] = 'Dasher'
    mapping[('LOWNDES','011')] = 'Jaycee Shack'
    mapping[('LOWNDES','001')] = 'Hahira Train Depot'
    mapping[('LOWNDES','002')] = 'Trinity'
    mapping[('LOWNDES','005')] = 'Rainwater'
    mapping[('LOWNDES','003')] = 'Northside'
    mapping[('LUMPKIN','DA')] = 'Dahlonega'
    mapping[('MCDUFFIE','131')] = '131'
    mapping[('MCDUFFIE','139')] = '139'
    mapping[('MCDUFFIE','136')] = '136'
    mapping[('MCDUFFIE','133A')] = '133A'
    mapping[('MCDUFFIE','137')] = '137'
    mapping[('MCDUFFIE','134')] = '134'
    mapping[('MCDUFFIE','132')] = '132'
    mapping[('MCDUFFIE','133B')] = '133B'
    mapping[('MCDUFFIE','135')] = '135'
    mapping[('MERIWETHER','04')] = 'Odessadale'
    mapping[('MERIWETHER','09')] = 'Gay'
    mapping[('MERIWETHER','12')] = 'Durand'
    mapping[('MERIWETHER','03')] = 'Gill II'
    mapping[('MERIWETHER','11')] = 'Warm Springs'
    mapping[('MERIWETHER','10')] = 'Woodbury'
    mapping[('MERIWETHER','08')] = 'Alvaton'
    mapping[('MERIWETHER','05')] = 'Greenville'
    mapping[('MILLER','1')] = 'Colquitt-Miller'
    mapping[('MORGAN','05')] = 'Clacks Chapel'
    mapping[('MORGAN','03')] = 'Beth/Springfield'
    mapping[('MORGAN','02')] = 'East Morgan'
    mapping[('MORGAN','07')] = 'North Morgan'
    mapping[('MORGAN','06')] = 'West Morgan'
    mapping[('MORGAN','01')] = 'Northeast Morgan'
    mapping[('MORGAN','04')] = 'Central Morgan'
    mapping[('MURRAY','872')] = 'Carters-Doolittle'
    mapping[('MURRAY','1013')] = 'McDonald'
    mapping[('MUSCOGEE','122')] = 'First African'
    mapping[('MUSCOGEE','125')] = 'Marianna Gallops'
    mapping[('MUSCOGEE','104')] = 'Britt David'
    mapping[('MUSCOGEE','107')] = 'Columbus Tech'
    mapping[('MUSCOGEE','126')] = 'Edgewood Baptist'
    mapping[('MUSCOGEE','109')] = 'Wynnbrook'
    mapping[('MUSCOGEE','121')] = 'Salvation Army'
    mapping[('MUSCOGEE','105')] = 'St Peter'
    mapping[('MUSCOGEE','102')] = 'Carver/Mack'
    mapping[('MUSCOGEE','110')] = 'Cusseta Rd'
    mapping[('MUSCOGEE','124')] = 'Epworth UMC'
    mapping[('MUSCOGEE','114')] = 'Faith Tabernacle'
    mapping[('MUSCOGEE','101')] = 'Wynnton/Britt'
    mapping[('MUSCOGEE','103')] = 'St John/Belvedere'
    mapping[('MUSCOGEE','117')] = 'Gentian/Reese'
    mapping[('MUSCOGEE','119')] = 'Moon/Morningside'
    mapping[('MUSCOGEE','113')] = 'Mt Pilgrim'
    mapping[('MUSCOGEE','112')] = 'Our Lady of Lourdes'
    mapping[('MUSCOGEE','115')] = 'Canaan'
    mapping[('MUSCOGEE','118')] = 'St Paul/Clubview'
    mapping[('MUSCOGEE','106')] = 'Cornerstone'
    mapping[('MUSCOGEE','116')] = 'Holsey/Buena Vista'
    mapping[('MUSCOGEE','108')] = 'St Mark/Heiferhorn'
    mapping[('MUSCOGEE','120')] = 'St Andrews/Midland'
    mapping[('MUSCOGEE','127')] = 'Psalmond/Mathews'
    mapping[('OCONEE','11')] = 'East Oconee'
    mapping[('OCONEE','10')] = 'Marswood Hall'
    mapping[('OCONEE','01')] = 'City Hall'
    mapping[('PAULDING','0016C')] = 'Beulahland Baptist Church'
    mapping[('PAULDING','0012C')] = 'Taylor Farm Park'
    mapping[('PAULDING','0018C')] = 'Picketts Mill Baptist Church'
    mapping[('PAULDING','0017C')] = 'Paulding County Airport'
    mapping[('PAULDING','0006B')] = 'Legacy Bapt Church'
    mapping[('PAULDING','0002C')] = 'Crossroads Library'
    mapping[('PAULDING','0009B')] = 'Events Place'
    mapping[('PAULDING','0010B')] = 'Poplar Sprgs Bapt Church'
    mapping[('PAULDING','0014B')] = 'White Oak Park'
    mapping[('PAULDING','0003C')] = 'Shelton Elementary'
    mapping[('PAULDING','0015C')] = 'Mulberry Rock Park'
    mapping[('PAULDING','0005C')] = 'Paulding Sr Center'
    mapping[('PAULDING','0001B')] = 'Burnt Hickory Park'
    mapping[('PAULDING','0011C')] = 'D Wright Innovation Ctr'
    mapping[('PAULDING','0019C')] = 'Dobbins Middle School'
    mapping[('PAULDING','0004C')] = 'Russom Elementary'
    mapping[('PAULDING','0013B')] = 'Nebo Elem School'
    mapping[('PAULDING','0007B')] = 'Watson Govt Cmplx'
    mapping[('PAULDING','0008C')] = 'West Ridge Church'
    mapping[('PEACH','F3')] = 'Fort Valley 3'
    mapping[('PEACH','F2')] = 'Fort Valley 2'
    mapping[('PEACH','F1')] = 'Fort Valley 1'
    mapping[('PEACH','B1')] = 'Byron 1'
    mapping[('PEACH','B2')] = 'Byron 2'
    mapping[('PICKENS','9')] = 'Tate'
    mapping[('PIERCE','4D')] = 'Sunset-Sweat'
    mapping[('PIERCE','3A')] = 'St Johns-Blackshear'
    mapping[('PIERCE','0001')] = 'Hacklebarney-Cason'
    mapping[('POLK','07')] = 'Youngs Grove'
    mapping[('POLK','05')] = 'Lake Creek'
    mapping[('POLK','06')] = 'Rockmart'
    mapping[('POLK','04')] = 'Fish Creek'
    mapping[('POLK','03')] = 'Cedartown'
    mapping[('POLK','02')] = 'Blooming Grove'
    mapping[('POLK','01')] = 'Aragon'
    mapping[('PULASKI','ANNEX')] = 'Courthouse Annex'
    mapping[('RABUN','RABUN')] = 'Rabun County'
    mapping[('RANDOLPH','718B')] = 'Cuthbert'
    mapping[('RANDOLPH','718A')] = 'Cuthbert-Courthouse'
    mapping[('ROCKDALE','LA')] = 'LA'
    mapping[('ROCKDALE','HC')] = 'HC'
    mapping[('ROCKDALE','CO')] = 'CO'
    mapping[('ROCKDALE','MA')] = 'MA'
    mapping[('ROCKDALE','BT')] = 'BT'
    mapping[('ROCKDALE','ST')] = 'ST'
    mapping[('ROCKDALE','HI')] = 'HI'
    mapping[('ROCKDALE','MI')] = 'MI'
    mapping[('ROCKDALE','FS')] = 'FS'
    mapping[('ROCKDALE','LO')] = 'LO'
    mapping[('ROCKDALE','RO')] = 'RO'
    mapping[('ROCKDALE','SM')] = 'SM'
    mapping[('ROCKDALE','BA')] = 'BA'
    mapping[('ROCKDALE','SP')] = 'SP'
    mapping[('ROCKDALE','OT')] = 'OT'
    mapping[('ROCKDALE','FI')] = 'FI'
    mapping[('SCREVEN','12')] = 'Cooperville Fire Station'
    mapping[('SCREVEN','08')] = 'Screven Rec Dept'
    mapping[('SCREVEN','04')] = 'Jenk Hill Fire Sta'
    mapping[('SPALDING','02')] = '02'
    mapping[('SPALDING','01')] = '01'
    mapping[('SPALDING','14')] = '14'
    mapping[('SPALDING','06')] = '06'
    mapping[('SPALDING','17')] = '17'
    mapping[('SPALDING','09')] = '09'
    mapping[('SPALDING','07')] = '07'
    mapping[('SPALDING','08')] = '08'
    mapping[('SPALDING','20')] = '20'
    mapping[('SPALDING','10')] = '10'
    mapping[('SPALDING','13')] = '13'
    mapping[('SPALDING','12')] = '12'
    mapping[('SPALDING','11')] = '11'
    mapping[('SPALDING','19')] = '19'
    mapping[('SPALDING','05')] = '05'
    mapping[('SPALDING','03')] = '03'
    mapping[('SPALDING','16')] = '16'
    mapping[('SPALDING','21')] = '21'
    mapping[('STEPHENS','SC1')] = 'Senior Center'
    mapping[('SUMTER','C1-27')] = 'Reese Park'
    mapping[('SUMTER','C2-27')] = 'GSW Conf Center'
    mapping[('SUMTER','C3-27')] = 'Rec Dept'
    mapping[('SUMTER','W-27')] = 'Agri-Center'
    mapping[('SUMTER','N-26')] = 'Concord'
    mapping[('SUMTER','17')] = 'Thompson'
    mapping[('SUMTER','28')] = 'Browns Mill'
    mapping[('SUMTER','O-26')] = 'Plains'
    mapping[('SUMTER','E-27')] = 'Airport'
    mapping[('TALBOT','01')] = 'ONeal'
    mapping[('TATTNALL','10')] = 'Shiloh'
    mapping[('TATTNALL','8')] = 'District IV'
    mapping[('TATTNALL','5')] = 'District II'
    mapping[('TELFAIR','LU')] = 'Lumber-City'
    mapping[('TELFAIR','MC')] = 'McRae'
    mapping[('TERRELL','06')] = 'Herod-Dover'
    mapping[('THOMAS','106')] = 'LIttle Ochlocknee Baptist Church'
    mapping[('THOMAS','118')] = 'Remington Esc'
    mapping[('TIFT','11')] = 'Mott-Litman Gym'
    mapping[('TOOMBS','514')] = '514 STIALC'
    mapping[('TOOMBS','513')] = '513 VPD'
    mapping[('TREUTLEN','ANNEX')] = 'Annex'
    mapping[('TROUP','02')] = 'Administration Bldg'
    mapping[('TROUP','10')] = 'McClendon'
    mapping[('TWIGGS','SG/T')] = 'Shady Grove/Tarvers'
    mapping[('TWIGGS','JV/W')] = 'Jeffersonville/Ware'
    mapping[('UPSON','1610')] = 'Redbone'
    mapping[('WALKER','0953')] = 'Armuchee Valley'
    mapping[('WALKER','0917')] = 'Walnut Grove'
    mapping[('WALKER','0871')] = 'La Fayette'
    mapping[('WALKER','1501')] = 'Chattanooga Valley'
    mapping[('WALKER','0944')] = 'Rock Spring'
    mapping[('WALTON','426')] = 'W Monroe'
    mapping[('WALTON','250')] = 'Walker Park'
    mapping[('WALTON','427')] = 'Tara'
    mapping[('WARE','404')] = '404'
    mapping[('WARE','407')] = '407'
    mapping[('WARE','408')] = '408'
    mapping[('WARE','405')] = '405'
    mapping[('WARE','200B')] = '200B'
    mapping[('WARE','409')] = '409'
    mapping[('WARE','406')] = '406'
    mapping[('WARE','200A')] = '200A'
    mapping[('WARE','100')] = '100'
    mapping[('WARE','400')] = '400'
    mapping[('WARE','304')] = '304'
    mapping[('WARE','300')] = '300'
    mapping[('WAYNE','4A')] = 'VFW'
    mapping[('WHITFIELD','TI')] = 'TI'
    mapping[('WHITFIELD','UT')] = 'UT'
    mapping[('WHITFIELD','CO')] = 'CO'
    mapping[('WHITFIELD','ES')] = 'ES'
    mapping[('WHITFIELD','LT')] = 'LT'
    mapping[('WHITFIELD','CA')] = 'CA'
    mapping[('WHITFIELD','TR')] = 'TR'
    mapping[('WHITFIELD','MC')] = 'MC'
    mapping[('WHITFIELD','FI')] = 'FI'
    mapping[('WHITFIELD','AN')] = 'AN'
    mapping[('WHITFIELD','NI')] = 'NI'
    mapping[('WHITFIELD','GL')] = 'GL'
    mapping[('WHITFIELD','WS')] = 'WS'
    mapping[('WHITFIELD','DG')] = 'DG'
    mapping[('WHITFIELD','TH')] = 'TH'
    mapping[('WHITFIELD','PG')] = 'PG'
    mapping[('WHITFIELD','VA')] = 'VA'
    mapping[('WILCOX','1A')] = 'Rochelle North 1'
    mapping[('WILCOX','2A')] = 'Abbeville North 2'
    mapping[('WILCOX','2B')] = 'Pineview 2'
    mapping[('WILCOX','3A')] = 'Pitts 3'
    mapping[('WILCOX','4A')] = 'Rochelle South 4'
    mapping[('WILCOX','5A')] = 'Abbeville South 5'
    mapping[('WILKES','2B')] = 'Metasville Fire Sta'
    mapping[('WILKES','4B')] = 'Tignal Sch Lunch Rm'
    mapping[('WILKES','3A')] = 'Edward B Pope Center'
    mapping[('WILKES','2A')] = 'Young Farmers Bldg'
    mapping[('WILKES','1')] = 'Senior Citizen Center'

    return mapping


def debug_precinct_mapping():
    precinct_mapping = load_precinct_mapping()
    early_voting = read_early_voting_data(35209) #nov 3 id
    election_data = read_election("/tmp/general_election_results_json.csv")
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
                    for x1, x2 in itertools.zip_longest(early_mismatches, contest_mismatches):
                        print(f"mapping[('{county.upper()}','{reverse_lookups.get(x1[0],x1[0])}')] = '{x2[0]}'")
                    raise
                election_precinct_info = contest[county]["votes"][precinct]["votes"]
                early_precinct_info = early_voting[county]["votes"][precinct]["votes"]
                for category in early_precinct_info["total"]:
                    projected_votes = early_precinct_info["total"][category]
                    actual_votes = sum([election_precinct_info[candidate][category] for candidate in election_precinct_info])
                    if ((projected_votes - actual_votes) / (actual_votes + 1e-6)) > 0.2:
                        print("Early versus actual discrepancy: ", county, precinct, category, projected_votes, actual_votes)


if __name__ == "__main__":
    #debug_precinct_mapping()

    early_voting = read_early_voting_data(35211) #jan 5 runoff

    baseline_contest = "US Senate (Perdue)"
    baseline_election_data = read_election("/tmp/general_election_results_json.csv")[baseline_contest]

    contest = "US Senate (Perdue)" #MUST: replace with name of runoff contest
    live_election_data = read_election("/tmp/general_election_results_json.csv", True)[contest]

    baseline_data = {}
    for county in baseline_election_data:
        for precinct in baseline_election_data[county]["votes"]:
            baseline_data.setdefault((county,precinct),{})
            baseline_precinct_data = baseline_election_data[county]["votes"][precinct]["votes"]
            cnt = {}
            rep = {}
            dem = {}
            for candidate in baseline_precinct_data:
                for category in baseline_precinct_data[candidate]:
                    if "(Rep)" in candidate:
                        rep[category] = rep.get(category,0) + baseline_precinct_data[candidate][category]
                    if "(Dem)" in candidate:
                        dem[category] = dem.get(category,0) + baseline_precinct_data[candidate][category]
                    cnt[category] = cnt.get(category,0) + baseline_precinct_data[candidate][category]
            for category in cnt:
                baseline_data[(county,precinct)].setdefault(category,{})
                baseline_data[(county,precinct)][category]["margin_pct"] = (rep[category]-dem[category]) / (rep[category]+dem[category]+1e-6)
                baseline_data[(county,precinct)][category]["rep"] = rep[category]
                baseline_data[(county,precinct)][category]["dem"] = dem[category]
                baseline_data[(county,precinct)][category]["total"] = cnt[category]
                baseline_data[(county,precinct)][category]["rep_dem_share"] = (rep[category]+dem[category]) / (cnt[category] + 1e-6)
            if (county, precinct) == ('Appling', '1B'):
                print(county, precinct, category, rep, dem, cnt)

    precinct_data = {}
    precinct_completion = {}
    for county in live_election_data:
        for precinct in live_election_data[county]["votes"]:
            precinct_data.setdefault((county,precinct),{})
            precinct_completion[(county,precinct)] = live_election_data[county]["votes"][precinct]["complete"]
            live_precinct_data = live_election_data[county]["votes"][precinct]["votes"]
            cnt = {}
            rep = {}
            dem = {}
            for candidate in live_precinct_data:
                for category in live_precinct_data[candidate]:
                    if "(Rep)" in candidate:
                        rep[category] = rep.get(category,0) + live_precinct_data[candidate][category]
                    if "(Dem)" in candidate:
                        dem[category] = dem.get(category,0) + live_precinct_data[candidate][category]
                    cnt[category] = cnt.get(category,0) + live_precinct_data[candidate][category]
            for category in cnt:
                #always pull margin from the baseline election
                precinct_data[(county,precinct)].setdefault(category,{})
                precinct_data[(county,precinct)][category]["margin_pct"] = (rep[category]-dem[category]) / (rep[category]+dem[category]+1e-6)
                precinct_data[(county,precinct)][category]["rep"] = rep[category]
                precinct_data[(county,precinct)][category]["dem"] = dem[category]
                precinct_data[(county,precinct)][category]["total"] = cnt[category]
                precinct_data[(county,precinct)][category]["rep_dem_share"] = (rep[category]+dem[category]) / (cnt[category] + 1e-6)
                #pull some turnout info from early voting and some from the baseline election
                if category in ["Advanced Voting Votes", "Absentee by Mail Votes"]:
                    if not precinct in early_voting[county]["votes"]:
                        print("No early data",county,precinct)
                        precinct_data[(county,precinct)][category]["proj_total"] = 0
                    else:
                        precinct_data[(county,precinct)][category]["proj_total"] = early_voting[county]["votes"][precinct]["total"][category]
                else:
                    precinct_data[(county,precinct)][category]["proj_total"] = cnt[category]
            if (county, precinct) == ('Appling', '1B'):
                print(county, precinct, category, rep, dem, cnt)


    #use loaded data for projections
    turnout_estimate = election_day_turnout(precinct_data, precinct_completion, baseline_data)
    election_day_ratio = turnout_estimate["ratio_to_baseline"]
    gen_completion_estimates(precinct_data, precinct_completion, baseline_data, election_day_ratio)

    #TODO: optionally overwrite completion_estimates with a certain amount outstanding by county or by category statewide
    #as sometimes this information comes in towards the end

    category_projections = get_category_projections(precinct_data, baseline_data)

    gen_precinct_projections(precinct_data, baseline_data, category_projections, election_day_ratio)

    export_fields = ["rep","dem","total","outs_rep","outs_dem","outs_total","proj_rep","proj_dem","proj_total"]

    #aggregate to county-level, state-level, including an extra aggregate category "total"
    state_data = {}
    county_data = {}
    for county,precinct in precinct_data:
        data = precinct_data[(county,precinct)]
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
    precinct_data = {"|".join(x):precinct_data[x] for x in precinct_data}

    #MUST: generate proper data instead of just copying
    with open("precinct-pred.json","w") as f_out:
        f_out.write(json.dumps({
            "perdue": precinct_data,
            "loeffler": precinct_data
        }));

    with open("pred.json","w") as f_out:
        f_out.write(json.dumps({
            "perdue": {
                "county": county_data,
                "state": state_data
            },
            "loeffler": {
                "county": county_data,
                "state": state_data
            }
        }))
