import requests
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from configparser import ConfigParser
import csv
import os

# Load config
config = ConfigParser()
config.read("config.ini")
BASE_URL = config["confluence"]["base_url"]
AUTH = (config["confluence"]["username"], config["confluence"]["password"])

# Settings
LOOK_BACK_DAYS = 30
LIMIT = 100
CSV_PATH = "user_activity.csv"
TOP_N = 20  # number of users to visualize
os.makedirs("plots", exist_ok=True)

requests.packages.urllib3.disable_warnings()

def get_all_users():
    users = []
    start = 0
    while True:
        url = f"{BASE_URL}/rest/api/user/search"
        params = {"start": start, "limit": LIMIT, "active": "true"}
        r = requests.get(url, params=params, auth=AUTH, verify=False)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        users.extend(batch)
        start += LIMIT
    return [u.get("username") or u.get("userKey") for u in users]

def get_user_activity(username, start_date, end_date):
    activity_dates = []
    start = 0
    while True:
        cql = (
            f"lastModified > {start_date.isoformat()} AND "
            f"lastModified <= {end_date.isoformat()} AND "
            f"lastModifier = \"{username}\""
        )
        url = f"{BASE_URL}/rest/api/content/search"
        params = {"cql": cql, "limit": LIMIT, "start": start}
        r = requests.get(url, params=params, auth=AUTH, verify=False)
        if r.status_code == 429:
            raise Exception("Rate limit hit — decorate this function")
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        for page in results:
            dt = page["version"]["when"]
            activity_dates.append(datetime.fromisoformat(dt[:10]))
        if len(results) < LIMIT:
            break
        start += LIMIT
    return activity_dates

def write_csv(user_date_counts):
    with open(CSV_PATH, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["username", "date", "count"])
        for user, date_counts in user_date_counts.items():
            for date, count in date_counts.items():
                writer.writerow([user, date.isoformat(), count])

def plot_top_users(user_date_counts):
    totals = {user: sum(counts.values()) for user, counts in user_date_counts.items()}
    top_users = sorted(totals.items(), key=lambda x: x[1], reverse=True)[:TOP_N]

    for user, _ in top_users:
        counts = user_date_counts[user]
        days = [datetime.now().date() - timedelta(days=i) for i in range(LOOK_BACK_DAYS)][::-1]
        values = [counts.get(d, 0) for d in days]
        fig, ax = plt.subplots(figsize=(10, 2))
        ax.bar(range(len(values)), values, width=1.0)
        ax.set_title(f"Activity for {user}")
        ax.set_xticks([])
        ax.set_yticks([])
        plt.tight_layout()
        plt.savefig(f"plots/user_activity_{user}.png")
        plt.close()

def main():
    start_date = datetime.now() - timedelta(days=LOOK_BACK_DAYS)
    end_date = datetime.now()

    print("Fetching users...")
    users = get_all_users()
    print(f"Found {len(users)} users")

    user_date_counts = defaultdict(Counter)

    for idx, username in enumerate(users):
        print(f"[{idx+1}/{len(users)}] Processing: {username}")
        try:
            activity = get_user_activity(username, start_date, end_date)
            for date in activity:
                user_date_counts[username][date.date()] += 1
        except Exception as e:
            print(f"Skipped {username}: {e}")

    print("Writing CSV...")
    write_csv(user_date_counts)

    print(f"Generating plots for top {TOP_N} users...")
    plot_top_users(user_date_counts)

if __name__ == "__main__":
    main()
