#!/usr/bin/env python3

import psycopg2
import os
import pandas as pd

# ✅ Function to Fetch Player Data
def get_player_data(player_name):
	conn = psycopg2.connect("mlb_data.db")
	query = "SELECT * FROM pitcher_stats WHERE player = ?"
	df = pd.read_sql(query, conn, params=(player_name,))
	conn.close()
	return df

# ✅ Function to Get All Players
def get_all_players():
	conn = psycopg2.connect("mlb_data.db")
	query = "SELECT DISTINCT player FROM pitcher_stats"
	df = pd.read_sql(query, conn)
	conn.close()
	return df["player"].tolist()

# ✅ Function to Get All Opponent Teams
def get_all_opponents():
	conn = psycopg2.connect("mlb_data.db")
	query = "SELECT DISTINCT opponent FROM pitcher_stats"
	df = pd.read_sql(query, conn)
	conn.close()
	return df["opponent"].tolist()

	