from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

BRONZE_PATH = BASE_DIR / "data" / "bronze"
SILVER_PATH = BASE_DIR / "data" / "silver"
GOLD_PATH = BASE_DIR / "data" / "gold"