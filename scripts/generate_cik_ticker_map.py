import sys
import os
import pandas as pd
import yfinance as yf
import time
import random
import config 

# --- Configuration ---
BLOCKHOLDERS_CSV = config.BLOCKHOLDERS_CSV
OUTPUT_CIK_TICKER_MAP_CSV = config.CIK_TICKER_MAP_CSV

# --- Mock/Fallback Data ---
HARDCODED_CIK_TICKER_SAMPLE = {
    '0000320193': {'ticker': 'AAPL', 'name': 'Apple Inc.'},
    '0000789019': {'ticker': 'XOM', 'name': 'Exxon Mobil'},
    '0001045810': {'ticker': 'GOOGL', 'name': 'Alphabet Inc.'},
    '0000076417': {'ticker': 'BRK-A', 'name': 'Berkshire Hathaway Inc.'},
    '0000034088': {'ticker': 'WMT', 'name': 'Walmart Inc.'},
    '0001090872': {'ticker': 'PFE', 'name': 'Pfizer Inc.'},
    '0000858102': {'ticker': 'MSFT', 'name': 'Microsoft Corp'},
    '0000049072': {'ticker': 'GE', 'name': 'General Electric Co.'},
    '0000062014': {'ticker': 'JPM', 'name': 'JPMorgan Chase & Co.'},
    '0000315213': {'ticker': 'CVX', 'name': 'Chevron Corp.'},
}

GENERIC_SECTORS = [
    "Technology", "Energy", "Financial Services", "Health Care",
    "Consumer Cyclical", "Consumer Defensive", "Industrials", "Utilities",
    "Materials", "Communication Services", "Real Estate"
]
GENERIC_LOCATIONS = [
    "New York", "California", "Texas", "Florida", "Washington",
    "Illinois", "Pennsylvania", "Ohio", "Georgia", "North Carolina",
    "Europe", "Asia"
]

def get_company_sector_location_from_yahoo(ticker: str) -> tuple[str | None, str | None]:
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

def generate_cik_ticker_map():
    print("--- Starting CIK-Ticker Map Generation ---")

    os.makedirs(os.path.dirname(OUTPUT_CIK_TICKER_MAP_CSV), exist_ok=True)

    unique_ciks = set()
    try:
        for chunk_df in pd.read_csv(BLOCKHOLDERS_CSV, chunksize=10000):
            unique_ciks.update(chunk_df['company_CIK'].dropna().astype(str).str.strip().unique())
        print(f"Extracted {len(unique_ciks)} unique CIKs from {BLOCKHOLDERS_CSV}.")
    except FileNotFoundError:
        print(f"ERROR: {BLOCKHOLDERS_CSV} not found. Please ensure it's in the 'data/' directory.")
        return

    cik_map_data = []
    
    yahoo_fetches = 0
    generic_assignments = 0

    for i, cik in enumerate(unique_ciks):
        if i % 100 == 0:
            print(f"Processing CIK {i}/{len(unique_ciks)}: {cik}")

        ticker = None
        sector = None
        location = None
        base_volatility = None
        company_name = f"Company_{cik}"

        if cik in HARDCODED_CIK_TICKER_SAMPLE:
            data = HARDCODED_CIK_TICKER_SAMPLE[cik]
            ticker = data['ticker']
            company_name = data['name']

        if ticker:
            try:
                yf_ticker_obj = yf.Ticker(ticker)
                info = yf_ticker_obj.info
                
                sector = info.get('sector')
                
                state = info.get('state')
                country = info.get('country')
                city = info.get('city')

                if state: location = state
                elif country: location = country
                elif city: location = city
                
                base_volatility = round(0.1 + random.random() * 0.2, 3) 
                
                if info.get('longName'):
                    company_name = info.get('longName')
                elif info.get('shortName'):
                    company_name = info.get('shortName')

                yahoo_fetches += 1
                time.sleep(0.5)

            except Exception as e:
                print(f"Warning: yfinance lookup failed for CIK {cik} / Ticker {ticker}: {e}. Assigning generic data.")
                ticker = None
        
        if not ticker or not sector or not location:
            ticker = ticker if ticker else "GENERIC"
            sector = sector if sector else random.choice(GENERIC_SECTORS)
            location = location if location else random.choice(GENERIC_LOCATIONS)
            base_volatility = base_volatility if base_volatility else round(0.1 + random.random() * 0.2, 3)
            generic_assignments += 1

        cik_map_data.append({
            'cik': cik,
            'name': company_name,
            'ticker': ticker,
            'sector': sector,
            'location': location,
            'base_volatility': base_volatility
        })

    cik_ticker_df = pd.DataFrame(cik_map_data)
    cik_ticker_df.to_csv(OUTPUT_CIK_TICKER_MAP_CSV, index=False)
    print(f"--- Generated CIK-Ticker Map: {OUTPUT_CIK_TICKER_MAP_CSV} ({len(cik_ticker_df)} entries) ---")
    print(f"  {yahoo_fetches} CIKs enriched via Yahoo Finance.")
    print(f"  {generic_assignments} CIKs assigned generic sector/location.")
    print("Consider sourcing a more comprehensive CIK-Ticker mapping for better accuracy.")

if __name__ == "__main__":
    generate_cik_ticker_map()