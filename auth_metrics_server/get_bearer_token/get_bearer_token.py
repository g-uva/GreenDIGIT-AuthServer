import os
import requests
from dotenv import load_dotenv, set_key

script_dir = os.path.dirname(os.path.abspath(__file__))

# Load existing .env (same folder by default)
env_local = os.path.join(script_dir, ".env")

# If not found, check parent directory
if os.path.isfile(env_local):
    ENV_PATH = env_local
else:
    ENV_PATH = os.path.join(os.path.dirname(script_dir), ".env")

load_dotenv(ENV_PATH)

email = os.environ.get("CIM_EMAIL")
password = os.environ.get("CIM_PASSWORD")
if not email or not password:
    raise SystemExit("CIM_EMAIL and CIM_PASSWORD must be set in .env")

base = os.environ.get("CIM_API_BASE", "https://mc-a4.lab.uvalight.net/gd-cim-api")
url = f"{base.rstrip('/')}/get-token"

r = requests.post(url, json={"email": email, "password": password}, timeout=10)
r.raise_for_status()
token = r.json()["access_token"]

# Write/replace JWT_TOKEN in the same .env
set_key(ENV_PATH, "JWT_TOKEN", token)
print("Updated JWT_TOKEN in", ENV_PATH)