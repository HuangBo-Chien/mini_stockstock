from dotenv import load_dotenv
import os

# 載入 .env 檔案
load_dotenv()

# 讀取 API 金鑰
api_key = os.getenv("OPENAI_API_KEY")
print(api_key)