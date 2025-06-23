import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# 🔐 Cargar las variables de conexión desde el archivo .env
load_dotenv()

usuario = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")

# 🧮 Llamar a la API de CoinGecko
url = 'https://api.coingecko.com/api/v3/coins/markets'
params = {
    'vs_currency': 'usd',
    'ids': 'bitcoin,ethereum,solana,cardano,dogecoin',
    'order': 'market_cap_desc',
    'per_page': '100',
    'page': '1',
    'sparkline': 'false'
}

response = requests.get(url, params=params)
data = response.json()

# Transformar el JSON en un dataframe de pandas
df = pd.DataFrame(data)

# Seleccionar los campos relevantes (los que creamos en la tabla Supabase)
df = df[[
    'symbol', 'name', 'current_price', 'market_cap', 'market_cap_rank',
    'total_volume', 'high_24h', 'low_24h',
    'price_change_24h', 'price_change_percentage_24h',
    'ath', 'atl', 'circulating_supply', 'total_supply',
    'max_supply', 'last_updated'
]]

# Convertir la columna de fecha al tipo timestamp de pandas
df['last_updated'] = pd.to_datetime(df['last_updated'])

# Conectar a la base de datos Supabase (PostgreSQL)
engine = create_engine(f'postgresql://{usuario}:{password}@{host}:5432/{database}')

# Insertar los datos en la tabla (añadiendo registros nuevos)
df.to_sql('crypto_prices', con=engine, if_exists='append', index=False)

print("✅ Datos insertados correctamente en Supabase")
