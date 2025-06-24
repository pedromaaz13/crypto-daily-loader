import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

# Crear conexión
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
)

# Obtener los symbols ya insertados hoy
today = datetime.utcnow().date()

query = f"""
SELECT symbol
FROM crypto_prices
WHERE load_date = '{today}';
"""

try:
    existing_symbols = pd.read_sql(query, con=engine)['symbol'].tolist()
except Exception as e:
    print(f"Error consultando Supabase: {e}")
    existing_symbols = []

# Obtener datos de CoinGecko
url = 'https://api.coingecko.com/api/v3/coins/markets'
params = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': '15',
    'page': '1',
    'sparkline': 'false'
}

response = requests.get(url, params=params)
response.raise_for_status()  # Si falla la API, lanza error

data = response.json()
df = pd.DataFrame(data)

# Seleccionar columnas
df = df[[
    'symbol', 'name', 'current_price', 'market_cap', 'market_cap_rank',
    'total_volume', 'high_24h', 'low_24h',
    'price_change_24h', 'price_change_percentage_24h',
    'ath', 'atl', 'circulating_supply', 'total_supply',
    'max_supply', 'last_updated'
]]

df['last_updated'] = pd.to_datetime(df['last_updated'])
df['load_date'] = today  # Fecha de carga (clave para evitar duplicados)

# Filtrar los que no estén ya insertados hoy
df_new = df[~df['symbol'].isin(existing_symbols)]

if df_new.empty:
    print("✅ No hay nuevos registros que insertar")
else:
    df_new.to_sql('crypto_prices', con=engine, if_exists='append', index=False)
    print(f"✅ {len(df_new)} registros nuevos insertados en Supabase")
