import psycopg2
import pandas as pd
import joblib
import numpy as np
import os
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

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
    
# ✅ Load Label Encoder for Opponent Encoding
encoder_path = "models/opponent_label_encoder.pkl"
label_encoder = joblib.load(encoder_path) if os.path.exists(encoder_path) else None

# ✅ Load Data from SQLite
conn = psycopg2.connect("mlb_data.db")
df = pd.read_sql("SELECT * FROM pitcher_stats WHERE opponent IS NOT NULL AND opponent != ''", conn)
df["opponent_k_rate"] = pd.to_numeric(df["opponent_k_rate"], errors="coerce")
df = df.dropna(subset=["opponent_k_rate"])  # ✅ Remove rows where opponent_k_rate is missing
conn.close()

# ✅ Convert Innings & Convert Strikeouts to Numeric
df["innings_pitched"] = df["innings_pitched"].apply(convert_innings)
df["strikeouts"] = pd.to_numeric(df["strikeouts"], errors="coerce")
df = df.dropna()

# ✅ Compute Recent K/9 for Each Player
df["recent_k9"] = df.groupby("player", group_keys=False)[["strikeouts", "innings_pitched"]].apply(
    lambda x: (x["strikeouts"].rolling(5, min_periods=1).sum() / x["innings_pitched"].rolling(5, min_periods=1).sum()) * 9
)

# ✅ Remove Empty Opponent Values Before Encoding
df = df[df["opponent"].notna() & (df["opponent"] != "")]

# ✅ Encode Opponents (Only If Label Encoder Exists)
if label_encoder:
    known_opponents = set(label_encoder.classes_)
    df = df[df["opponent"].isin(known_opponents)]
    df["opponent_encoded"] = label_encoder.transform(df["opponent"])
    
# ✅ Prepare to Store Results
results = []

# ✅ Define Feature Names for Consistency
feature_names = ["innings_pitched", "opponent_encoded", "home_away", "opponent_k_rate", "recent_k9"]

# ✅ Loop Through Each Pitcher
players = df["player"].unique().tolist()
for player in players:
    model_path = f"models/{player}_model.pkl"
    
    if os.path.exists(model_path):
        model = joblib.load(model_path)
        player_data = df[df["player"] == player]
        
        # ✅ Make Predictions for Each Game
        for index, row in player_data.iterrows():
            # ✅ Handle Missing `recent_k9` and `opponent_k_rate`
            opponent_k_rate = row["opponent_k_rate"] if not np.isnan(row["opponent_k_rate"]) else player_data["opponent_k_rate"].mean()
            recent_k9 = row["recent_k9"] if not np.isnan(row["recent_k9"]) else player_data["recent_k9"].mean()
            
            # ✅ Convert X_test to Pandas DataFrame with Feature Names
            X_test = pd.DataFrame([[row["innings_pitched"], row["opponent_encoded"], row["home_away"], opponent_k_rate, recent_k9]], columns=feature_names)
            y_actual = row["strikeouts"]
            
            y_pred = model.predict(X_test)[0]  
            
            results.append({
                "player": player,
                "date": row["dat"],
                "opponent": row["opponent"],
                "innings_pitched": row["innings_pitched"],
                "actual_strikeouts": y_actual,
                "predicted_strikeouts": round(y_pred, 2),
                "opponent_k_rate": round(opponent_k_rate, 3),
                "recent_k9": round(recent_k9, 2)
            })
            
# ✅ Convert Results to DataFrame
results_df = pd.DataFrame(results)

# ✅ Calculate Accuracy Metrics
mae = mean_absolute_error(results_df["actual_strikeouts"], results_df["predicted_strikeouts"])
mse = mean_squared_error(results_df["actual_strikeouts"], results_df["predicted_strikeouts"])
r2 = r2_score(results_df["actual_strikeouts"], results_df["predicted_strikeouts"])

print(f"📊 **Model Accuracy Metrics:**")
print(f"🔹 Mean Absolute Error (MAE): {round(mae, 2)}")
print(f"🔹 Mean Squared Error (MSE): {round(mse, 2)}")
print(f"🔹 R² Score: {round(r2, 2)}")

# ✅ Save results to a CSV for further analysis
results_df.to_csv("model_evaluation_results.csv", index=False)
print("✅ Accuracy results saved to 'model_evaluation_results.csv'.")

