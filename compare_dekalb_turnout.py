import csv
import requests

def scrape_dekalb_turnout():
    #this website shows live dekalb turnout on the day of the election: https://dekalbgis.maps.arcgis.com/
    reader = csv.DictReader(open("precinct_mapping.csv"))

    writer = csv.DictWriter(open("dekalb_turnout.csv","w"), fieldnames=["time","count","precinct"])
    writer.writeheader()

    dekalb_precincts = {}
    for row in reader:
        if row["county"] == "DEKALB":
            dekalb_precincts[row["label"]] = row["precinct"]

    for abbrev in dekalb_precincts:
        params = (
            ('f', 'json'), #website requests in pbf, not dealing with protobuf for now
            ('where', f" (facilityid = '{abbrev}') "),
            ('returnGeometry', 'false'),
            ('spatialRel', ''),
            ('outFields', 'CreationDate,TotalCount,OBJECTID'),
            ('resultOffset', '0'),
            ('resultRecordCount', '1000'),
            ('quantizationParameters', '{"mode":"edit"}'),
        )

        for data in requests.get('https://services2.arcgis.com/IxVN2oUE9EYLSnPE/arcgis/rest/services/Election_Day_Geography/FeatureServer/3/query', params=params).json()["features"]:
            writer.writerow({
                "time": data["attributes"]["CreationDate"],
                "count": data["attributes"]["TotalCount"],
                "precinct": dekalb_precincts[abbrev]
            })

def compare_turnout():
    baseline_turnout = {}
    live_turnout = {}

    for row in csv.DictReader(open("/tmp/election_results_nov_3.csv")):
        if row["county"] == "DeKalb" and row["category"] == "Election Day Votes":
            baseline_turnout[row["precinct"]] = baseline_turnout.get(row["precinct"],0) + int(row["votes"])

    for row in csv.DictReader(open("dekalb_turnout.csv")):
        if int(row["count"]) > int(live_turnout.get(row["precinct"],-1)):
            live_turnout[row["precinct"]] = int(row["count"])

    total_baseline_turnout = sum(baseline_turnout[x] for x in baseline_turnout)
    total_live_turnout = sum(live_turnout[x] for x in live_turnout)

    print(total_live_turnout, total_baseline_turnout, total_live_turnout / total_baseline_turnout)


if __name__ == "__main__":
    scrape_dekalb_turnout()
    compare_turnout()
