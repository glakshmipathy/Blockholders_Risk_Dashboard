import sys
import os
import pandas as pd
import random
import config 

# --- Configuration ---
OUTPUT_FEMA_RISK_MAP_CSV = config.FEMA_RISK_MAP_CSV

# --- Hardcoded US State-based Climate Risk Data (Simulates FEMA/NOAA processing) ---
US_STATE_CLIMATE_RISK_DATA = {
    "Florida": {"Climate Risk": 0.9, "Hurricane Risk": 0.8, "Flood Risk": 0.7},
    "Louisiana": {"Climate Risk": 0.95, "Hurricane Risk": 0.9, "Flood Risk": 0.85},
    "Texas": {"Climate Risk": 0.7, "Heat Risk": 0.8, "Flood Risk": 0.6},
    "California": {"Climate Risk": 0.75, "Earthquake Risk": 0.8, "Wildfire Risk": 0.7, "Drought Risk": 0.6},
    "New York": {"Climate Risk": 0.4, "Flood Risk": 0.5, "Winter Storm Risk": 0.4},
    "Washington": {"Climate Risk": 0.5, "Earthquake Risk": 0.6, "Rainfall Risk": 0.4},
    "Oregon": {"Climate Risk": 0.45, "Earthquake Risk": 0.55, "Wildfire Risk": 0.4},
    "North Carolina": {"Climate Risk": 0.6, "Hurricane Risk": 0.4, "Flood Risk": 0.5},
    "South Carolina": {"Climate Risk": 0.6, "Hurricane Risk": 0.45, "Flood Risk": 0.55},
    "Mississippi": {"Climate Risk": 0.8, "Hurricane Risk": 0.7, "Flood Risk": 0.75},
    "Alabama": {"Climate Risk": 0.7, "Hurricane Risk": 0.6, "Flood Risk": 0.65},
    "Oklahoma": {"Climate Risk": 0.6, "Tornado Risk": 0.8},
    "Kansas": {"Climate Risk": 0.5, "Tornado Risk": 0.7},
    "Missouri": {"Climate Risk": 0.4, "Flood Risk": 0.3},
    "Nebraska": {"Climate Risk": 0.2, "Drought Risk": 0.3},
    "Arkansas": {"Climate Risk": 0.3, "Flood Risk": 0.35},
    "Massachusetts": {"Climate Risk": 0.3, "Winter Storm Risk": 0.35},
    "Illinois": {"Climate Risk": 0.25, "Flood Risk": 0.25},
    "Ohio": {"Climate Risk": 0.2, "Winter Storm Risk": 0.2},
    "Europe": {"Geopolitical Risk": 0.2, "Regulatory Risk": 0.15},
    "Asia": {"Geopolitical Risk": 0.3, "Supply Chain Disruption": 0.4},
    "Unknown": {"General Operational Risk": 0.1}
}

def generate_fema_risk_map():
    """
    Generates data/fema_risk_by_location.csv with pre-defined climate risks for various locations.
    This simulates fetching and processing NOAA/FEMA data.
    """
    print("--- Starting FEMA/NOAA Risk Map Generation ---")

    os.makedirs(os.path.dirname(OUTPUT_FEMA_RISK_MAP_CSV), exist_ok=True)

    risk_data = []
    for location, risks in US_STATE_CLIMATE_RISK_DATA.items():
        for risk_type, base_score in risks.items():
            score_with_variation = max(0.0, min(1.0, base_score + (random.uniform(-0.05, 0.05))))
            risk_data.append({
                "location": location,
                "risk_type": risk_type,
                "risk_score": round(score_with_variation, 3)
            })
    
    fema_risk_df = pd.DataFrame(risk_data)
    fema_risk_df.to_csv(OUTPUT_FEMA_RISK_MAP_CSV, index=False)
    print(f"--- Generated FEMA/NOAA Risk Map: {OUTPUT_FEMA_RISK_MAP_CSV} ({len(fema_risk_df)} entries) ---")

if __name__ == "__main__":
    generate_fema_risk_map()