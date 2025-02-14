#!/usr/bin/env python3

import streamlit as st
import pandas as pd
import joblib
import sqlite3
import os
import numpy as np

# ✅ Function to Convert Innings to Proper Fraction
def convert_innings(innings):
	""" Convert innings pitched (6.1 -> 6.333, 6.2 -> 6.667) """
	parts = str(innings).split(".")
	whole = int(parts[0])  # Main innings count
	decimal_part = int(parts[1]) if len(parts) > 1 else 0  # Out count
	
	if decimal_part == 1:
		return whole + (1/3)  # 1 out = 1/3 inning
	elif decimal_part == 2:
		return whole + (2/3)  # 2 outs = 2/3 inning
	else:
		return whole  # No decimal part, return whole number
	
# ✅ Load Label Encoder for Opponent Encoding
encoder_path = "models/opponent_label_encoder.pkl"
if os.path.exists(encoder_path):
	label_encoder = joblib.load(encoder_path)
else:
	label_encoder = None
	
# ✅ Get Player List from Database
def get_players():
	conn = sqlite3.connect("mlb_data.db")
	df = pd.read_sql("SELECT DISTINCT player FROM pitcher_stats", conn)
	conn.close()
	return df["player"].tolist()

# ✅ Get Opponent List from Database
def get_opponents():
	conn = sqlite3.connect("mlb_data.db")
	df = pd.read_sql("SELECT DISTINCT opponent FROM pitcher_stats", conn)
	conn.close()
	return df["opponent"].dropna().tolist()

# ✅ Get Player's Game Logs
def get_player_game_logs(player):
	conn = sqlite3.connect("mlb_data.db")
	df = pd.read_sql(f"SELECT * FROM pitcher_stats WHERE player = '{player}' ORDER BY dat DESC", conn)
	conn.close()
	
	# ✅ Convert home/away column (0 → "H", 1 → "A")
	df["home_away"] = df["home_away"].map({0: "H", 1: "A"})
	
	return df

# ✅ Calculate K/9 (Strikeouts per 9 innings)
def calculate_k_per_9(df):
	""" Calculates strikeouts per 9 innings (K/9) """
	df = df.copy()
	df["innings_pitched"] = df["innings_pitched"].apply(convert_innings)
	df["strikeouts"] = pd.to_numeric(df["strikeouts"], errors="coerce")
	df = df.dropna()
	
	total_innings = df["innings_pitched"].sum()
	total_strikeouts = df["strikeouts"].sum()
	
	if total_innings > 0:
		return round((total_strikeouts / total_innings) * 9, 2)
	return None  # Return None if there's no valid data

# ✅ Streamlit UI
st.title("MLB Pitcher Strikeout Predictor")

# ✅ User Inputs
player = st.selectbox("Select a Player", get_players())

# ✅ Only Load Model if It Exists
model_path = f"models/{player}_model.pkl"
if os.path.exists(model_path):
	model = joblib.load(model_path)
	
	# ✅ Dropdown for Opponent
	opponent = st.selectbox("Select Opponent", get_opponents())
	
	# ✅ Dropdown for Valid Inning Choices
	valid_innings = [i for i in range(10)] + [i + 0.1 for i in range(10)] + [i + 0.2 for i in range(10)]
	innings_pitched = st.selectbox("Select Innings Pitched", valid_innings)
	
	# ✅ Dropdown for Home/Away Selection
	home_away = st.radio("Home or Away?", ["Home", "Away"])
	home_away_value = 1 if home_away == "Away" else 0  # Convert to numeric (1 = Away, 0 = Home)
	
	# ✅ Convert Innings to Correct Format Before Prediction
	innings_pitched = convert_innings(innings_pitched)
	
	# ✅ Get Player's Game Logs
	player_games = get_player_game_logs(player)
	
	# ✅ Calculate Season K/9 (All Games)
	season_k9 = calculate_k_per_9(player_games)
	
	# ✅ Calculate Last 5 Games K/9
	last_5_games_k9 = calculate_k_per_9(player_games.head(5))
	
	player_games["opponent_k_rate"] = pd.to_numeric(player_games["opponent_k_rate"], errors="coerce")
	
	# ✅ Compute the mean K% while ignoring invalid values
	opponent_k_rate = player_games[player_games["opponent"] == opponent]["opponent_k_rate"].mean()
	
	
	# ✅ Predict Strikeouts
	if st.button("Predict Strikeouts"):
		# ✅ Calculate Last 5 Games K/9 for Recent Form
		recent_k9 = calculate_k_per_9(player_games.head(5))
		if recent_k9 is None:
			recent_k9 = season_k9  # Use season K/9 as a fallback
		if label_encoder and opponent in label_encoder.classes_:
			opponent_encoded = label_encoder.transform([opponent])[0]
			# ✅ Make Sure We Have 5 Features with Explicit Column Names
			X_pred = pd.DataFrame([[innings_pitched, opponent_encoded, home_away_value, opponent_k_rate, recent_k9]],
														columns=["innings_pitched", "opponent_encoded", "home_away", "opponent_k_rate", "recent_k9"])
			# ✅ Make Prediction
			prediction = model.predict(X_pred)[0]
			st.success(f"Predicted Strikeouts: {round(prediction, 2)}")
			
			# ✅ Display K/9 Stats
			st.subheader(f"📊 {player} K/9 Stats")
			st.write(f"🔹 **Season K/9:** {season_k9 if season_k9 is not None else 'Not Available'}")
			st.write(f"🔹 **Last 5 Games K/9:** {last_5_games_k9 if last_5_games_k9 is not None else 'Not Available'}")
			
		else:
			st.error("Opponent not found in training data. Try another.")
			
	# ✅ Display Last 5 Games (All Stats, Home/Away as 'H'/'A')
	st.subheader(f"📊 {player}'s Last 5 Games (All Stats)")
	last_5_games = player_games.head(5)
	st.dataframe(last_5_games)  # ✅ Shows all columns, including formatted home/away
	
	# ✅ Display Player's Games vs. Chosen Opponent (Without Scroll)
	st.subheader(f"📊 {player}'s Games vs. {opponent}")
	games_vs_opponent = player_games[player_games["opponent"] == opponent]
	
	if not games_vs_opponent.empty:
		st.table(games_vs_opponent)  # ✅ Removes scrolling
	else:
		st.warning(f"No previous games found for {player} vs. {opponent}")
		
else:
	st.warning(f"No model available for {player}. Try another player.")
	