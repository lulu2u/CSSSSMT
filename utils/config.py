import os 
from pathlib import Path 
from dotenv import load_dotenv 
 
load_dotenv() 
BASE_DIR = Path(__file__).parent.parent 
 
class Config: 
    SECRET_KEY = 'meituan-web-secret-key' 
    DEBUG = False 
    PORT = 5000 
    @staticmethod 
    def get_brand_config_path(): 
        return BASE_DIR / 'config' / 'brands.yaml' 
