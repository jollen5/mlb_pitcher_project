#!/usr/bin/env python3

import sqlite3
import pandas as pd
import joblib
import numpy as np
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder

# ✅ Function to Convert Innings to Proper Fraction
def convert_innings(innings):
	""" Convert innings pitched (6.1 -> 6.333, 6.2 -> 6.667) """
	parts = str(innings).split(".")
	whole = int(parts[0])  
	decimal_part = int(parts[1]) if len(parts) > 1 else 0  
	
	if decimal_part == 1:
		return whole + (1/3)  
	elif decimal_part == 2:
		return whole + (2/3)  
	else:
		return whole  
	
# ✅ Load Data from SQLite
conn = sqlite3.connect("mlb_data.db")
df = pd.read_sql("SELECT * FROM pitcher_stats", conn)
conn.close()

# ✅ Convert Innings & Convert Strikeouts to Numeric
df["innings_pitched"] = df["innings_pitched"].apply(convert_innings)
df["strikeouts"] = pd.to_numeric(df["strikeouts"], errors="coerce")

# ✅ Ensure "models/" directory exists
if not os.path.exists("models"):
	os.makedirs("models")
	
# ✅ Load or Create Label Encoder for Opponent Encoding
encoder_path = "models/opponent_label_encoder.pkl"
if os.path.exists(encoder_path):
	label_encoder = joblib.load(encoder_path)
	print("✅ Loaded existing label encoder.")
else:
	print("⚠️ No label encoder found. Creating a new one...")
	label_encoder = LabelEncoder()
	df = df[df["opponent"].notna() & (df["opponent"] != "")]  # Remove empty values
	df["opponent_encoded"] = label_encoder.fit_transform(df["opponent"])
	joblib.dump(label_encoder, encoder_path)  # Save the new encoder
	print("✅ New label encoder created and saved.")
	
# ✅ Remove Rows Where Opponent is Missing or Unseen
df = df[df["opponent"].notna() & (df["opponent"] != "")]
known_opponents = set(label_encoder.classes_)
df = df[df["opponent"].isin(known_opponents)]  # Keep only known opponents
df["opponent_encoded"] = label_encoder.transform(df["opponent"])

# ✅ Compute Average Innings Pitched Per Game for Each Pitcher
df["avg_ip_per_game"] = df.groupby("player")["innings_pitched"].transform("mean")

# ✅ Remove Relief Pitchers (Avg IP < 3.0)
df = df[df["avg_ip_per_game"] >= 3.0]

# ✅ Compute Recent Form (Last 5 Games K/9) - FIXED
df["recent_k9"] = df.groupby("player", group_keys=False)[["strikeouts", "innings_pitched"]].apply(
	lambda x: (x["strikeouts"].rolling(5, min_periods=1).sum() / x["innings_pitched"].rolling(5, min_periods=1).sum()) * 9
)


# ✅ Get Unique Players
players = df["player"].unique().tolist()

for player in players:
	player_data = df[df["player"] == player]
	
	if player_data.shape[0] > 5:  # ✅ Ensure Enough Data
		player_data = player_data.copy()  # ✅ Fix SettingWithCopyWarning
		player_data = player_data.dropna()
	
		# ✅ Define Features (Includes Opponent, Home/Away, Opponent K%, and Recent Form)
		X = player_data[["innings_pitched", "opponent_encoded", "home_away", "opponent_k_rate", "recent_k9"]]
		y = player_data["strikeouts"]
	
		# ✅ Train-Test Split
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
	
		# ✅ Train Model
		model = RandomForestRegressor(n_estimators=100, random_state=42)
		model.fit(X_train, y_train)
	
		# ✅ Save Model per Player
		joblib.dump(model, f"models/{player}_model.pkl")
		print(f"✅ Model trained for {player}")
	else:
		print(f"⚠️ Not enough data for {player} to train a model.")
		
		
		