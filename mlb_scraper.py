import requests
import psycopg2
import os
import threading
import time
import random
import concurrent.futures
from bs4 import BeautifulSoup
import pandas as pd

# ✅ Global Variable to Stop Threads Gracefully
stop_scraping = threading.Event()

# ✅ List of User-Agents to Avoid Detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.48",
]

# ✅ Ensure Database and Table Exist Before Scraping
def ensure_database():
    conn = psycopg2.connect("mlb_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pitcher_stats (
            player TEXT,
            dat TEXT,
            home_away INTEGER,
            opponent TEXT,
            innings_pitched TEXT,
            earned_runs TEXT,
            strikeouts TEXT,
            walks TEXT,
            pitch_count TEXT
        );
    """)
    conn.commit()
    conn.close()
    print("✅ Database setup complete.")
    
# ✅ Team Name to Abbreviation Mapping (Manually Defined)
TEAM_ABBREVIATIONS = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago White Sox": "CWS",
    "Chicago Cubs": "CHC",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Yankees": "NYY",
    "New York Mets": "NYM",
    "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH"
}

# ✅ Scrape Team K% (Opponent Strikeout Rate) with Abbreviations
def scrape_team_k_rates():
    print("🔄 Scraping team K% (Opponent Strikeout Rate)...")
    url = "https://www.baseball-reference.com/leagues/majors/2024-advanced-batting.shtml"
    session = requests.Session()
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    
    try:
        response = session.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            print(f"⚠️ Error: Unable to access team batting stats. Status Code: {response.status_code}")
            return
        
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"id": "teams_advanced_batting"})  # ✅ Correct table ID
        
        if not table:
            print("⚠️ Error: Team advanced batting stats table not found.")
            return
        
        team_k_data = []
        for row in table.find_all("tr")[1:]:  # ✅ Skip header row
            team_name_cell = row.find("th", {"data-stat": "team_name"})  # ✅ Find team name in <th>
            cols = row.find_all("td")  # ✅ Find numerical columns
        
            if team_name_cell and len(cols) > 5:  # ✅ Ensure data exists
                full_team_name = team_name_cell.text.strip()  # ✅ Extract Full Team Name
                team_abbreviation = TEAM_ABBREVIATIONS.get(full_team_name, full_team_name)  # ✅ Convert to Abbreviation
                so_percent = cols[5].text.strip()  # ✅ Extract SO% from correct column
        
                # ✅ Exclude aggregate rows (AL/NL Totals)
                if "Total" in full_team_name or full_team_name == "":
                    continue
        
                if so_percent:
                    team_k_data.append({"team": team_abbreviation, "opponent_k_rate": float(so_percent.replace("%", "")) / 100})
                    
        # ✅ Save to CSV
        import pandas as pd
        team_k_df = pd.DataFrame(team_k_data)
        team_k_df.to_csv("team_k_rates.csv", index=False)
        print("✅ Team K% data saved to 'team_k_rates.csv' with abbreviations.")
        
    except Exception as e:
        print(f"⚠️ Error scraping team K%: {e}")
        
# ✅ Function to get all MLB pitcher IDs
def get_pitcher_ids():
    url = "https://www.baseball-reference.com/leagues/majors/2024-standard-pitching.shtml"
    session = requests.Session()
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    
    retries = 6
    for attempt in range(retries):
        try:
            response = session.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                wait_time = (2 ** attempt) + random.uniform(10, 20)
                print(f"⏳ Too Many Requests (429), retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                headers["User-Agent"] = random.choice(USER_AGENTS)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error: {e}")
            continue
        
    if response.status_code != 200:
        print(f"⚠️ Error: Unable to access the page. Status Code: {response.status_code}")
        return {}
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "players_standard_pitching"})
    if table is None:
        print("⚠️ Error: Could not find the table. The page structure may have changed.")
        return {}
    
    rows = table.find_all("tr")
    pitcher_ids = {}
    
    for row in rows:
        link = row.find("a")
        if link:
            name = link.text.strip()
            player_id = link["href"].split("/")[-1].replace(".shtml", "")
            pitcher_ids[name] = player_id
            
    return pitcher_ids

# ✅ Function to scrape individual pitcher data
def scrape_pitcher_data(player_id, player_name):
    session = requests.Session()
    url = f"https://www.baseball-reference.com/players/gl.fcgi?id={player_id}&t=p&year=2024"
    
    retries = 6
    for attempt in range(retries):
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        time.sleep(random.uniform(15, 30))
        
        try:
            response = session.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                wait_time = (2 ** attempt) + random.uniform(20, 40)
                print(f"⏳ Too Many Requests for {player_name}, retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                headers["User-Agent"] = random.choice(USER_AGENTS)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error for {player_name}: {e}")
            continue
        
    if response.status_code != 200:
        print(f"❌ Skipping {player_name}, failed after {retries} retries.")
        return
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "pitching_gamelogs"})
    if not table:
        print(f"⚠️ No data found for {player_name}")
        return
    
    rows = table.find_all("tr")
    pitcher_data = []
    
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 21:
            try:
                dat = cols[2].text.strip()
                home_away = cols[4].text.strip()
                opponent = cols[5].text.strip()
                innings = cols[10].text.strip()
                earned_runs = cols[13].text.strip()
                strikeouts = cols[15].text.strip()
                walks = cols[14].text.strip()
                pitch_count = cols[21].text.strip()
                
                home_away = 1 if home_away == "@" else 0
                pitcher_data.append((player_name, dat, home_away, opponent, innings, earned_runs, strikeouts, walks, pitch_count))
                
            except Exception as e:
                print(f"⚠️ Skipping row for {player_name} due to error: {e}")
                
    if pitcher_data:
        conn = psycopg2.connect("mlb_data.db")
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT INTO pitcher_stats (player, dat, home_away, opponent, innings_pitched, earned_runs, strikeouts, walks, pitch_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            pitcher_data,
        )
        conn.commit()
        conn.close()
        print(f"✅ Finished scraping for {player_name}")
        
# ✅ Multi-threaded scraping function
def scrape_all_pitchers(pitcher_ids):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(scrape_pitcher_data, pid, name) for name, pid in pitcher_ids.items()]
        concurrent.futures.wait(futures)
        
# ✅ Run Everything in Correct Order
ensure_database()
scrape_team_k_rates()
print("⏳ Waiting 5 seconds before scraping pitchers...")
time.sleep(5)
pitcher_ids = get_pitcher_ids()
if pitcher_ids:
    scrape_all_pitchers(pitcher_ids)
    print("✅ All 2024 pitcher data successfully scraped!")
    
    