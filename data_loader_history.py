import os
import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar las variables de entorno desde el .env
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

# Crear la conexión con SQLAlchemy
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
)

# Lista de criptos (puedes añadir o quitar las que quieras)
coins = [
    'bitcoin',
    'ethereum',
    'solana',
    'cardano',
    'dogecoin',
    'avalanche-2',
    'tron',
    'polkadot',
    'litecoin',
    'chainlink',
    'polygon',
    'internet-computer',
    'stellar',
    'monero',
    'vechain'
]

# Número de días hacia atrás (365 días = 1 año)
days = 365

# Función para obtener histórico
def get_historical_data(coin_id, days):
    url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart'
    params = {
        'vs_currency': 'usd',
        'days': days
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"❌ Error con {coin_id}: {response.status_code}")
        return None
    
    data = response.json()

    # A veces la API responde vacío
    if 'prices' not in data:
        print(f"⚠️ No hay datos de precios para {coin_id}")
        return None

    prices = data['prices']
    market_caps = data['market_caps']
    total_volumes = data['total_volumes']

    # Construimos DataFrame
    df = pd.DataFrame(prices, columns=['timestamp', 'price'])
    df['market_cap'] = [mc[1] for mc in market_caps]
    df['total_volume'] = [tv[1] for tv in total_volumes]
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date
    df['symbol'] = coin_id

    return df[['symbol', 'price', 'market_cap', 'total_volume', 'date']]

# DataFrame total donde vamos acumulando
historical_df = pd.DataFrame()

# Loop por cada moneda
for coin in coins:
    df_coin = get_historical_data(coin, days)
    if df_coin is not None:
        historical_df = pd.concat([historical_df, df_coin], ignore_index=True)
    time.sleep(1)  # Pausa de 1 segundo entre llamadas para respetar el API

# Añadimos el nombre de la moneda (con una pequeña tabla auxiliar)
coin_names = {
    'bitcoin': 'Bitcoin',
    'ethereum': 'Ethereum',
    'solana': 'Solana',
    'cardano': 'Cardano',
    'dogecoin': 'Dogecoin',
    'avalanche-2': 'Avalanche',
    'tron': 'Tron',
    'polkadot': 'Polkadot',
    'litecoin': 'Litecoin',
    'chainlink': 'Chainlink',
    'polygon': 'Polygon',
    'internet-computer': 'Internet Computer',
    'stellar': 'Stellar',
    'monero': 'Monero',
    'vechain': 'VeChain'
}

historical_df['name'] = historical_df['symbol'].map(coin_names)

# Comprobamos que no haya duplicados antes de insertar
historical_df.drop_duplicates(subset=['symbol', 'date'], inplace=True)

# Insertamos los datos en la tabla
# IMPORTANTE: Tu tabla en Supabase debe tener:
# UNIQUE(symbol, date)
# Para evitar duplicados en inserciones

try:
    historical_df.to_sql('crypto_history', con=engine, if_exists='append', index=False)
    print("✅ Datos históricos insertados correctamente")
except Exception as e:
    print(f"⚠️ Error insertando datos: {e}")
