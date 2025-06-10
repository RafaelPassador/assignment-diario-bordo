import os
from dotenv import load_dotenv

load_dotenv()

BASE_PATH = os.getenv("BASE_PATH", "./data")
INPUT_PATH = os.getenv("INPUT_PATH", os.path.join(BASE_PATH, "input", "info_transportes.csv"))
BRONZE_PATH = os.getenv("BRONZE_PATH", os.path.join(BASE_PATH, "bronze"))
SILVER_PATH = os.getenv("SILVER_PATH", os.path.join(BASE_PATH, "silver"))
GOLD_PATH = os.getenv("GOLD_PATH", os.path.join(BASE_PATH, "gold"))