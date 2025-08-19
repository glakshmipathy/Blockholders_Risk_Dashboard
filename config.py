import os
from dotenv import load_dotenv

# Load environment variables (from .env file)
load_dotenv()

# --- Database Configuration ---
MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "neo4j")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "password")

# --- File Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')

BLOCKHOLDERS_CSV = os.path.join(DATA_DIR, 'blockholders.csv')
MARKET_CAP_CSV = os.path.join(DATA_DIR, 'market_cap.csv')
CIK_TICKER_MAP_CSV = os.path.join(DATA_DIR, 'cik_ticker_map.csv')
FEMA_RISK_MAP_CSV = os.path.join(DATA_DIR, 'fema_risk_by_location.csv')

OUTPUT_RISK_EXPOSURES_CSV = os.path.join(OUTPUT_DIR, "company_risk_exposures.csv")
OUTPUT_METADATA_ENRICHED_CSV = os.path.join(OUTPUT_DIR, "company_metadata_enriched.csv")

# --- Pipeline Parameters ---
CHUNK_SIZE = 10000
MAX_RISK_ITERATIONS = 15
RISK_CONCENTRATION_THRESHOLD = 0.3
TOP_N_CRITICAL_NODES = 10

# --- App & UI Parameters ---
LLM_ENABLED = True if os.getenv("GEMINI_API_KEY") else False
LLM_MODEL_NAME = "gemini-1.5-flash"
DEFAULT_YEAR_FILTER = 2023

START_YEAR = 2022
END_YEAR = 2023

GRAPH_MAX_NODES = 300
GRAPH_MAX_NODES_RANGE = (50, 500)
GRAPH_MAX_NODES_STEP = 50
RISK_THRESHOLD = 0.1
MIN_EXPOSURE_WEIGHT = 0.2