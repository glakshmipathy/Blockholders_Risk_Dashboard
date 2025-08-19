import pandas as pd
import os
import yfinance as yf
import numpy as np
import time
import random
from dotenv import load_dotenv
import config

load_dotenv()

STATIC_SECTOR_MARKET_RISK_MAP = {
    "Technology": 0.65,
    "Energy": 0.80,
    "Financial Services": 0.70,
    "Health Care": 0.55,
    "Consumer Cyclical": 0.60,
    "Consumer Defensive": 0.40,
    "Industrials": 0.50,
    "Utilities": 0.30,
    "Materials": 0.55,
    "Communication Services": 0.60,
    "Real Estate": 0.45,
    "Automotive": 0.75,
    "General": 0.50,
    "Non-Profit": 0.10
}

GENERIC_SECTORS = [
    "Technology", "Energy", "Financial Services", "Health Care",
    "Consumer Cyclical", "Consumer Defensive", "Industrials", "Utilities",
    "Materials", "Communication Services", "Real Estate", "General", "Non-Profit"
]
GENERIC_LOCATIONS = [
    "New York", "California", "Texas", "Florida", "Washington",
    "Illinois", "Pennsylvania", "Ohio", "Georgia", "North Carolina",
    "Europe", "Asia", "Unknown"
]


def get_company_sector_location_from_yahoo(ticker: str) -> tuple[str | None, str | None]:
    """
    Attempts to fetch company sector and primary location (state/country) from Yahoo Finance.
    This is used as a fallback if the CIK-ticker map does not contain this information.
    """
    try:
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info
        sector = info.get('sector')
        
        state = info.get('state')
        country = info.get('country')
        city = info.get('city')

        location = None
        if state:
            location = state
        elif country:
            location = country
        elif city:
            location = city
            
        return sector, location
    except Exception as e:
        print(f"Warning: Failed to fetch sector/location for ticker {ticker} from Yahoo Finance: {e}")
        return None, None

def get_fema_risk_from_map(location: str, fema_risk_df: pd.DataFrame) -> dict:
    """Looks up geographic risk from a pre-loaded FEMA DataFrame."""
    risks = fema_risk_df[fema_risk_df['location'].astype(str).str.lower() == location.lower()]
    if not risks.empty:
        return risks.set_index('risk_type')['risk_score'].to_dict()
    return {}


def automate_enrichment_pipeline():
    """
    Automates the entire enrichment process:
    1. Reads unique CIKs from blockholders.csv.
    2. Maps CIKs to tickers and gets base company info from generated cik_ticker_map.csv.
    3. Fetches missing sector/location from Yahoo Finance (if not in map).
    4. Assigns 'Market Volatility' and 'Geographic' risks to companies using static maps.
    5. Generates output CSVs for risk exposures and enriched company metadata.
    """
    print("--- Starting automated enrichment pipeline (using static market risk data) ---")

    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(config.CIK_TICKER_MAP_CSV):
        print(f"ERROR: CIK-Ticker map CSV not found at {config.CIK_TICKER_MAP_CSV}.")
        print("Please run `python run_pipeline.py --gen-cik-map` first.")
        return

    cik_ticker_df = pd.read_csv(config.CIK_TICKER_MAP_CSV, dtype={'cik': str})
    cik_map_data_loaded = cik_ticker_df.set_index('cik').to_dict(orient='index')
    print(f"Loaded CIK-Ticker map for {len(cik_map_data_loaded)} entries.")

    if not os.path.exists(config.FEMA_RISK_MAP_CSV):
        print(f"ERROR: FEMA risk map CSV not found at {config.FEMA_RISK_MAP_CSV}.")
        print("Please run `python run_pipeline.py --gen-fema-map` first.")
        return
    fema_risk_df = pd.read_csv(config.FEMA_RISK_MAP_CSV)
    print(f"Loaded FEMA geographic risk data for {len(fema_risk_df)} entries.")


    unique_blockholder_companies_cik = set()
    try:
        for chunk_df in pd.read_csv(config.BLOCKHOLDERS_CSV, chunksize=config.CHUNK_SIZE):
            unique_blockholder_companies_cik.update(
                chunk_df['company_CIK'].dropna().astype(str).str.strip().unique()
            )
        print(f"Found {len(unique_blockholder_companies_cik)} unique company CIKs in {config.BLOCKHOLDERS_CSV}.")
    except FileNotFoundError:
        print(f"ERROR: Blockholders CSV not found at {config.BLOCKHOLDERS_CSV}.")
        return


    companies_enriched_attributes = {}
    for i, cik in enumerate(unique_blockholder_companies_cik):
        if i % 1000 == 0:
            print(f"Processing company CIK {i}/{len(unique_blockholder_companies_cik)}...")

        company_id_graph = "C_" + cik
        
        company_info = cik_map_data_loaded.get(cik, {})

        ticker = company_info.get('ticker')
        sector = company_info.get('sector')
        location = company_info.get('location')
        base_volatility = company_info.get('base_volatility')
        company_name = company_info.get('name', f"Company_{cik}")

        if ticker and (not sector or not location):
            fetched_sector, fetched_location = get_company_sector_location_from_yahoo(ticker)
            if fetched_sector and not sector: sector = fetched_sector
            if fetched_location and not location: location = fetched_location
            if (fetched_sector or fetched_location) : time.sleep(0.5)

        if not sector: sector = random.choice(GENERIC_SECTORS)
        if not location: location = random.choice(GENERIC_LOCATIONS)
        if base_volatility is None: base_volatility = round(0.05 + random.random() * 0.25, 3)

        companies_enriched_attributes[cik] = {
            'company_id_graph': company_id_graph,
            'name': company_name,
            'ticker': ticker,
            'sector': sector,
            'location': location,
            'volatility': base_volatility
        }

    all_company_risks = []
    enriched_company_metadata_output = []

    print("\n--- Assigning risks to companies based on enriched attributes ---")
    for cik, data in companies_enriched_attributes.items():
        company_id_graph = data['company_id_graph']
        company_name = data['name']
        company_sector = data['sector']
        company_location = data['location']
        company_volatility = data['volatility'] 

        all_company_risks.append({
            "company_id": company_id_graph,
            "company_name": company_name,
            "risk_factor": "Inherent Market Volatility",
            "risk_weight": round(company_volatility, 3)
        })

        sector_market_risk_weight = STATIC_SECTOR_MARKET_RISK_MAP.get(company_sector, STATIC_SECTOR_MARKET_RISK_MAP["General"])
        all_company_risks.append({
            "company_id": company_id_graph,
            "company_name": company_name,
            "risk_factor": f"Sector Market Risk ({company_sector})",
            "risk_weight": round(sector_market_risk_weight, 3)
        })
        
        if company_location:
            location_risks = get_fema_risk_from_map(company_location, fema_risk_df)
            for risk_type, weight in location_risks.items():
                all_company_risks.append({
                    "company_id": company_id_graph,
                    "company_name": company_name,
                    "risk_factor": risk_type,
                    "risk_weight": round(weight, 3)
                })

        enriched_company_metadata_output.append({
            'company_id_graph': company_id_graph,
            'company_name': company_name,
            'sector': company_sector,
            'location': company_location,
            'volatility': company_volatility
        })
    print("--- Finished assigning risks to companies ---")

    pd.DataFrame(all_company_risks).to_csv(config.OUTPUT_RISK_EXPOSURES_CSV, index=False)
    print(f"Generated company risk exposures CSV: {config.OUTPUT_RISK_EXPOSURES_CSV}")

    pd.DataFrame(enriched_company_metadata_output).to_csv(config.OUTPUT_METADATA_ENRICHED_CSV, index=False)
    print(f"Generated enriched company metadata CSV: {config.OUTPUT_METADATA_ENRICHED_CSV}")

if __name__ == "__main__":
    automate_enrichment_pipeline()