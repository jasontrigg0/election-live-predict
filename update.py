#main loop that checks for new data and pushed to github as necessary
import os
import sys
import subprocess
import datetime
import time
sys.path.append(os.path.dirname(__file__)) #allow import from current directory
from scraper import update_nov_3_election_data as update_election_data #MUST: change to jan 5
from predict import generate_predictions

if __name__ == "__main__":
    while True:
        print("Checking for new data...")
        found_new_data = update_election_data()
        if found_new_data:
            print("Found new data")
            print("Generating predictions")
            generate_predictions()
            print("Adding commit and pushing to github")
            subprocess.check_output(["git", "add", os.path.join(os.path.dirname(__file__),'pred.json'), os.path.join(os.path.dirname(__file__),'precinct-pred.json')])
            subprocess.check_output(["git", "commit", "-m", f"Automated prediction update: {datetime.datetime.now()}"])
            subprocess.check_output(["git", "push"])
        time.sleep(10)
