import sys
import os
import pandas as pd
import numpy as np
import random
from dotenv import load_dotenv
import config

# Load environment variables
load_dotenv()

# Define paths
BLOCKHOLDERS_CSV = config.BLOCKHOLDERS_CSV
OUTPUT_MARKET_CAP_CSV = config.MARKET_CAP_CSV

def generate_market_cap_data():
    """
    Generates a CSV file with mock market capitalization data for all unique companies
    found in the blockholders dataset.
    """
    print("--- Starting Market Cap Data Generation ---")

    os.makedirs(os.path.dirname(OUTPUT_MARKET_CAP_CSV), exist_ok=True)

    unique_company_ciks = set()
    try:
        for chunk_df in pd.read_csv(BLOCKHOLDERS_CSV, chunksize=10000):
            unique_company_ciks.update(chunk_df['company_CIK'].dropna().astype(str).str.strip().unique())
        print(f"Found {len(unique_company_ciks)} unique company CIKs in {BLOCKHOLDERS_CSV}.")
    except FileNotFoundError:
        print(f"ERROR: {BLOCKHOLDERS_CSV} not found. Please ensure it's in the 'data/' directory.")
        return

    market_cap_data = []
    # Using a seeded random number generator for consistent results
    random.seed(42)

    for cik in unique_company_ciks:
        # Generate a realistic-looking market cap in billions of dollars
        # Corrected: Changed 'lognormalvariate' to 'lognormvariate'
        market_cap_billion = max(0.1, random.lognormvariate(2, 1.5))
        market_cap_dollars = round(market_cap_billion * 1_000_000_000, 2)
        
        company_id_graph = "C_" + cik
        
        market_cap_data.append({
            'company_id_graph': company_id_graph,
            'market_cap': market_cap_dollars
        })

    market_cap_df = pd.DataFrame(market_cap_data)
    market_cap_df.to_csv(OUTPUT_MARKET_CAP_CSV, index=False)
    print(f"--- Generated Market Cap Data: {OUTPUT_MARKET_CAP_CSV} ({len(market_cap_df)} entries) ---")

if __name__ == "__main__":
    generate_market_cap_data()