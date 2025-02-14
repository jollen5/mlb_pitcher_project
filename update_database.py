import sqlite3
import pandas as pd

# ✅ Load Opponent K% Data
team_k_df = pd.read_csv("team_k_rates.csv")
team_k_dict = dict(zip(team_k_df["team"], team_k_df["opponent_k_rate"]))

# ✅ Connect to SQLite Database
conn = sqlite3.connect("mlb_data.db")
cursor = conn.cursor()

# ✅ Retrieve All Pitcher Data
df = pd.read_sql("SELECT * FROM pitcher_stats WHERE opponent IS NOT NULL AND opponent != ''", conn)

# ✅ Team Abbreviation Mapping (Fixing Naming Differences)
TEAM_NAME_FIXES = {
    "TBR": "TB",  # Example: Convert "CWS" (White Sox) to "CHW"
    "SFG": "SF",  # Example: Convert "WSH" (Nationals) to "WAS"
    "KCR": "KC",
    "CHW": "CWS",
    "WSN": "WSH",
    "SDP": "SD",
    # Add more fixes as needed
}

# ✅ Apply Fixes to Opponent Column Before Mapping
df["opponent"] = df["opponent"].replace(TEAM_NAME_FIXES)

# ✅ Map Opponent K% to Each Row
df["opponent_k_rate"] = df["opponent"].map(team_k_dict)

# ✅ Update the Database with New K% Values
for index, row in df.iterrows():
    cursor.execute(
        "UPDATE pitcher_stats SET opponent_k_rate = ? WHERE player = ? AND dat = ?",
        (row["opponent_k_rate"], row["player"], row["dat"]),
    )
    
# ✅ Commit and Close Connection
conn.commit()
conn.close()
print("✅ Opponent K% successfully added to pitcher_stats table.")