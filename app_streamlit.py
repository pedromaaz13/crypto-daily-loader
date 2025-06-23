import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import plotly.express as px

# ===========================
# FUNCIONES DE FORMATEO
# ===========================

def format_dollar(value, decimals=2):
    try:
        return f"${value:,.{decimals}f}"
    except:
        return value

def format_percent(value, decimals=2):
    try:
        return f"{value:.{decimals}f}%"
    except:
        return value

# ===========================
# CONEXIÓN A LA BASE DE DATOS
# ===========================

# Cargar las variables del .env
load_dotenv()

usuario = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")

# Conexión a Supabase
engine = create_engine(f'postgresql://{usuario}:{password}@{host}:5432/{database}')

# ===========================
# CARGA DE DATOS
# ===========================

query = """
SELECT 
    symbol, name, current_price, market_cap, market_cap_rank,
    total_volume, high_24h, low_24h,
    price_change_24h, price_change_percentage_24h,
    ath, atl, circulating_supply, total_supply,
    max_supply, last_updated
FROM crypto_prices
ORDER BY last_updated DESC
"""
df = pd.read_sql(query, con=engine)

# Eliminar duplicados por símbolo (si hay varias cargas)
df = df.drop_duplicates(subset=['symbol'], keep='last')

# ===========================
# CÁLCULO DE NUEVOS RATIOS
# ===========================

df['cap_vol_ratio'] = df['market_cap'] / df['total_volume']
df['supply_ratio'] = (df['circulating_supply'] / df['total_supply']) * 100

# ===========================
# STREAMLIT - VISUALIZACIÓN
# ===========================

st.set_page_config(page_title="Cripto Dashboard", page_icon="💰", layout="wide")
st.title("🚀 Cripto Dashboard Personal")

# Fecha de actualización
ultima_fecha = df['last_updated'].max()
st.write(f"📅 Última actualización: {ultima_fecha}")

# ===========================
# FILTRO INTERACTIVO
# ===========================

monedas = df['name'].unique().tolist()
seleccionadas = st.multiselect("Selecciona criptomonedas para comparar:", monedas, default=monedas)
df_filtrado = df[df['name'].isin(seleccionadas)]

# ===========================
# KPIs
# ===========================

st.subheader("💰 Precios actuales")

kpi_cols = st.columns(len(df_filtrado))
for i, row in enumerate(df_filtrado.itertuples()):
    precio = format_dollar(row.current_price)
    kpi_cols[i].metric(row.name, precio)

# ===========================
# GRÁFICO PRECIOS ACTUALES
# ===========================

st.subheader("📊 Comparativa de precios actuales")

fig_precio = px.bar(
    df_filtrado,
    x='name',
    y='current_price',
    color='name',
    text=df_filtrado['current_price'].map(format_dollar),
    labels={'current_price': 'Precio USD'},
    title="Precio actual por moneda"
)
fig_precio.update_layout(yaxis_tickprefix='$', yaxis_tickformat=',.2f')
st.plotly_chart(fig_precio, use_container_width=True)

# ===========================
# GRÁFICO VOLUMEN 24H
# ===========================

st.subheader("📈 Volumen transaccionado (24h)")

fig_volumen = px.bar(
    df_filtrado,
    x='name',
    y='total_volume',
    color='name',
    text=df_filtrado['total_volume'].map(lambda x: format_dollar(x, 0)),
    labels={'total_volume': 'Volumen USD'},
    title="Volumen 24h"
)
fig_volumen.update_layout(yaxis_tickprefix='$', yaxis_tickformat=',.0f')
st.plotly_chart(fig_volumen, use_container_width=True)

# ===========================
# GRÁFICO VARIACIÓN 24H
# ===========================

st.subheader("📊 Variación de precios últimas 24 horas")

df_sorted = df_filtrado.sort_values('price_change_percentage_24h', ascending=False)

fig_variacion = px.bar(
    df_sorted,
    x='name',
    y='price_change_percentage_24h',
    color='name',
    text=df_sorted['price_change_percentage_24h'].map(format_percent),
    labels={'price_change_percentage_24h': '% Cambio 24h'},
    title="Cambio porcentual en 24h"
)
fig_variacion.update_layout(yaxis_tickformat=',.2f')
st.plotly_chart(fig_variacion, use_container_width=True)

# ===========================
# RATIOS ADICIONALES ULTRA PRO
# ===========================

st.subheader("📊 Ratios adicionales")

col1, col2 = st.columns(2)

# Columna 1: Capitalización / Volumen
with col1:
    st.markdown("🔸 **Capitalización / Volumen**  \n(Relación entre capitalización de mercado y volumen transaccionado en 24h. A menor ratio, mayor liquidez relativa)")
    df_cap_vol = df_filtrado[['name', 'cap_vol_ratio']].sort_values('cap_vol_ratio', ascending=False).reset_index(drop=True)
    st.dataframe(
        df_cap_vol.style.format({'cap_vol_ratio': format_dollar}),
        use_container_width=True
    )

# Columna 2: % Supply Circulante
with col2:
    st.markdown("🔸 **% Supply circulante**  \n(Porcentaje de oferta total que ya está en circulación. A mayor porcentaje, menor emisión futura disponible)")
    df_supply = df_filtrado[['name', 'supply_ratio']].sort_values('supply_ratio', ascending=False).reset_index(drop=True)
    st.dataframe(
        df_supply.style.format({'supply_ratio': format_percent}),
        use_container_width=True
    )

# ===========================
# TABLA FINAL DE DETALLE
# ===========================

st.subheader("📄 Detalle de datos")

df_formatted = df_filtrado.copy()
df_formatted['current_price'] = df_formatted['current_price'].map(format_dollar)
df_formatted['market_cap'] = df_formatted['market_cap'].map(format_dollar)
df_formatted['total_volume'] = df_formatted['total_volume'].map(lambda x: format_dollar(x, 0))
df_formatted['price_change_24h'] = df_formatted['price_change_24h'].map(format_dollar)
df_formatted['price_change_percentage_24h'] = df_formatted['price_change_percentage_24h'].map(format_percent)
df_formatted['ath'] = df_formatted['ath'].map(format_dollar)
df_formatted['atl'] = df_formatted['atl'].map(format_dollar)

st.dataframe(df_formatted.reset_index(drop=True), use_container_width=True)
