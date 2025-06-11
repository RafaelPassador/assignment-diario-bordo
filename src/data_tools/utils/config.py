from dataclasses import dataclass
from src.config import INPUT_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH

@dataclass
class DataConfig:
    input_path: str = INPUT_PATH
    bronze_path: str = BRONZE_PATH
    silver_path: str = SILVER_PATH
    gold_path: str = GOLD_PATH
    
    def get_path_for_layer(self, layer: str) -> str:
        paths = {
            'input': self.input_path,
            'bronze': self.bronze_path,
            'silver': self.silver_path,
            'gold': self.gold_path
        }
        return paths.get(layer.lower(), '')
