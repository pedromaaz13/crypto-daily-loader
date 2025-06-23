import os
import requests
import pandas as pd
from sqlalchemy import create_engine

# 🔐 Leer las variables de conexión desde variables de entorno (GitHub Actions)
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

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

# Seleccionar los campos relevantes (los que tenemos en la tabla Supabase)
df = df[[
    'symbol', 'name', 'current_price', 'market_cap', 'market_cap_rank',
    'total_volume', 'high_24h', 'low_24h',
    'price_change_24h', 'price_change_percentage_24h',
    'ath', 'atl', 'circulating_supply', 'total_supply',
    'max_supply', 'last_updated'
]]

# Convertir la columna de fecha al tipo timestamp de pandas
df['last_updated'] = pd.to_datetime(df['last_updated'])

# Crear el engine de conexión a Supabase (PostgreSQL)
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
)

# Insertar los datos en la tabla (añadiendo registros nuevos)
df.to_sql('crypto_prices', con=engine, if_exists='append', index=False)

print("✅ Datos insertados correctamente en Supabase")


