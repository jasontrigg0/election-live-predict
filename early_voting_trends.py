import csv
import datetime

def days_before_election(date_string, election_date):
    try:
        month, day, year = [int(x) for x in date_string.split("/")]
        return ((election_date - datetime.date(year, month, day)).days)
    except:
        return None


def election_early_vote_trends(early_vote_file, election_date):
    reader = csv.DictReader(open(early_vote_file,encoding = "ISO-8859-1"))
    daily_mail_counts = {}
    daily_early_counts = {}
    for row in reader:
        if row["Ballot Style"] == "MAILED" and row["Ballot Status"] in ["A"]:
            daily_mail_counts[row["Ballot Return Date"]] = daily_mail_counts.get(row["Ballot Return Date"],0) + 1
        elif row["Ballot Style"] == "IN PERSON":
            daily_early_counts[row["Ballot Return Date"]] = daily_early_counts.get(row["Ballot Return Date"],0) + 1
    #print(sorted([(days_before_election(x,election_date), daily_mail_counts[x]) for x in daily_mail_counts], key = lambda x: -999 if x[0] is None else x[0]))
    print(sorted([(days_before_election(x,election_date), daily_early_counts[x]) for x in daily_early_counts], key = lambda x: -999 if x[0] is None else x[0]))


if __name__ == "__main__":
    election_early_vote_trends("/home/jason/Downloads/35211/STATEWIDE.csv", datetime.date(2021,1,5))
    election_early_vote_trends("/home/jason/Downloads/35209/STATEWIDE.csv", datetime.date(2020,11,3))
