import subprocess
import os
import sys
import argparse
import config
from modules.logging_utils import logger
from modules.db_loader import DBLoader
from modules.risk_engine import RiskEngine
from scripts.generate_cik_ticker_map import generate_cik_ticker_map
from scripts.generate_fema_risk_map import generate_fema_risk_map
from scripts.generate_market_cap import generate_market_cap_data
from data_enricher import automate_enrichment_pipeline

def main(clear_db=True):
    """
    Executes the full data pipeline automatically.
    """
    loader = None
    engine = None
    try:
        logger.info("--- Starting full automated pipeline ---")
        
        loader = DBLoader(uri=config.MEMGRAPH_URI, user=config.MEMGRAPH_USER, password=config.MEMGRAPH_PASSWORD)
        engine = RiskEngine(uri=config.MEMGRAPH_URI, user=config.MEMGRAPH_USER, password=config.MEMGRAPH_PASSWORD)

        if clear_db:
            logger.info("--- Clearing all data from Memgraph ---")
            loader.driver.session().run("MATCH (n) DETACH DELETE n")
            logger.info("--- Database cleared. ---")
            
            # --- ADDING INDEXES FOR PERFORMANCE ---
            logger.info("--- Creating indexes for faster data loading ---")
            with loader.driver.session() as session:
                session.run("CREATE INDEX ON :Company(id)")
                session.run("CREATE INDEX ON :Blockholder(id)")
                session.run("CREATE INDEX ON :RiskFactor(name)")
            logger.info("--- Indexes created. ---")

        logger.info("--- Generating data CSVs ---")
        generate_cik_ticker_map()
        generate_fema_risk_map()
        generate_market_cap_data()

        logger.info("--- Loading blockholders from CSV ---")
        loader.load_blockholders(config.BLOCKHOLDERS_CSV, chunk_size=config.CHUNK_SIZE, start_year=config.START_YEAR, end_year=config.END_YEAR)

        logger.info("--- Running automated data enrichment ---")
        automate_enrichment_pipeline()

        logger.info("--- Loading enriched metadata and market cap into DB ---")
        loader.load_enriched_company_metadata(config.OUTPUT_METADATA_ENRICHED_CSV)
        loader.load_market_cap_data(config.MARKET_CAP_CSV)

        logger.info("--- Loading EXPOSED_TO relationships into DB ---")
        loader.load_risk_exposures_from_csv(config.OUTPUT_RISK_EXPOSURES_CSV)

        logger.info("--- Computing and propagating risks ---")
        engine.compute_total_risk(max_iterations=config.MAX_RISK_ITERATIONS)
        engine.dollarize_risk()

        logger.info("--- Pipeline execution complete. ---")

    except Exception as e:
        logger.error("FATAL ERROR during pipeline execution", exc_info=True)
        return False
    finally:
        if loader:
            loader.close()
        if engine:
            engine.close()
    return True

if __name__ == "__main__":
    if main():
        sys.exit(0)
    else:
        sys.exit(1)