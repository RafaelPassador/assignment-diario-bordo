import os
from dotenv import load_dotenv

load_dotenv()

# Nomes das tabelas
BRONZE_TABLE = os.getenv("BRONZE_TABLE", "b_info_transportes")
SILVER_TABLE = os.getenv("SILVER_TABLE", "s_info_transportes")
GOLD_TABLE = os.getenv("GOLD_TABLE", "info_corridas_do_dia")

# Caminhos base
BASE_PATH = os.path.abspath(os.getenv("BASE_PATH", "./data"))
INPUT_PATH = os.path.abspath(os.getenv("INPUT_PATH", os.path.join(BASE_PATH, "input", "info_transportes.csv")))

# Caminhos com nomes das tabelas incluídos
BRONZE_PATH = os.path.abspath(os.getenv("BRONZE_PATH", os.path.join(BASE_PATH, "bronze", BRONZE_TABLE)))
SILVER_PATH = os.path.abspath(os.getenv("SILVER_PATH", os.path.join(BASE_PATH, "silver", SILVER_TABLE)))
GOLD_PATH = os.path.abspath(os.getenv("GOLD_PATH", os.path.join(BASE_PATH, "gold", GOLD_TABLE)))