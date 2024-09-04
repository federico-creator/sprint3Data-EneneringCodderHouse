import requests
import pandas as pd
from datetime import datetime
import time
import psycopg2
from psycopg2.extras import execute_values
#from dotenv import load_dotenv
#import os

cryptos = ["bitcoin", "ethereum", "tether","ripple", "litecoin", "solana","cardano","dogecoin", "chainlink", "polkadot", "dai"]

#load_dotenv()
password = "nf32F7968i"
url = "https://api.coingecko.com/api/v3/coins/markets"
urlRedshift = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
database = "data-engineer-database"
user = "bornicofederico_coderhouse"
params = {
    'vs_currency': 'usd',
    'ids': ','.join(cryptos)
}
def connect_to_redshift():
    try:
        conn = psycopg2.connect(
            host=urlRedshift,
            dbname=database,
            user=user,
            password=password,
            port='5439'
        )
        print("Conección correcta con REDSHIFT")
        return conn
    except Exception as e:
        print("Error en la conexión a REDSHIFT")
        print(e)
        return None

# Se modifica la base de datos para que exista una primary key compuesta (Symbol, que es el simbolo de la crypto y DateTime)
def create_table(conn):    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crypto_data (
                    ID VARCHAR(255),
                    Symbol VARCHAR(50) NOT NULL,
                    Name VARCHAR(255) NOT NULL,
                    Current_Price DECIMAL(18, 8) NOT NULL,
                    Market_Cap DECIMAL(38, 2) NOT NULL,
                    Total_Volume DECIMAL(38, 2),
                    High_24h DECIMAL(18, 8),
                    Low_24h DECIMAL(18, 8),
                    Price_Change_24h DECIMAL(18, 8),
                    Price_Change_Percentage_24h DECIMAL(5, 2),
                    Market_Cap_Change_24h DECIMAL(38, 2),
                    Market_Cap_Change_Percentage_24h DECIMAL(5, 2),
                    Circulating_Supply DECIMAL(38, 2),
                    Total_Supply DECIMAL(38, 2),
                    Ath DECIMAL(18, 8),
                    Ath_Change_Percentage DECIMAL(5, 2),
                    DateTime TIMESTAMP,
                    PRIMARY KEY (Symbol, DateTime)
                );
            """)
            conn.commit()
            print("Se creo la tabla correctamente")
    except Exception as e:
        print("No se pudo crear la tabla")
        print(e)

def get_data():
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener datos. Reintentando en 10 minutos")
        time.sleep(10 * 60)
        return process_data()

def process_data():
    crypto_data = get_data()
    current_datetime = datetime.now().strftime("%Y-%m-%d") # se obtiene solo el día, ya que la info se solicita cada 3 horas, esto hace que el dato del día se actualice cada 3 horas
    crypto_list = []
    for crypto in crypto_data:
        crypto_info = {
            'ID': crypto.get('id'),
            'Symbol': crypto.get('symbol'),
            'Name': crypto.get('name'),
            'Current_Price': crypto.get('current_price'),
            'Market_Cap': crypto.get('market_cap'),
            'Total_Volume': crypto.get('total_volume'),
            'High_24h': crypto.get('high_24h'),
            'Low_24h': crypto.get('low_24h'),
            'Price_Change_24h': crypto.get('price_change_24h'),
            'Price_Change_Percentage_24h': crypto.get('price_change_percentage_24h'),
            'Market_Cap_Change_24h': crypto.get('market_cap_change_24h'),
            'Market_Cap_Change_Percentage_24h': crypto.get('market_cap_change_percentage_24h'),
            'Circulating_Supply': crypto.get('circulating_supply'),
            'Total_Supply': crypto.get('total_supply'),
            'Ath': crypto.get('ath'),
            'Ath_Change_Percentage': crypto.get('ath_change_percentage'),
            'DateTime': current_datetime
        }
        crypto_list.append(crypto_info)
    df = pd.DataFrame(crypto_list)
    return df

def cargar_datos(conn, df):
    df = df.dropna(subset=['Symbol', 'Name', 'Current_Price'])

    df.fillna(value={
        'Market_Cap': 'desconocido',
        'Total_Volume': 'desconocido',
        'High_24h': 'desconocido',
        'Low_24h': 'desconocido',
        'Price_Change_24h': 'desconocido',
        'Price_Change_Percentage_24h': 'desconocido',
        'Market_Cap_Change_24h': 'desconocido',
        'Market_Cap_Change_Percentage_24h': 'desconocido',
        'Circulating_Supply': 'desconocido',
        'Total_Supply': 'desconocido',
        'Ath': 'desconocido',
        'Ath_Change_Percentage': 'desconocido',
    }, inplace=True)

    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                DELETE FROM crypto_data WHERE DateTime::date = %s::date;
                """,
                [(row[16],) for row in df.itertuples(index=False, name=None)],
                page_size=len(df)
            )

            execute_values(
                cur,
                """
                INSERT INTO crypto_data (
                    ID,Symbol, Name, Current_Price, Market_Cap,
                    Total_Volume, High_24h, Low_24h, Price_Change_24h,
                    Price_Change_Percentage_24h, Market_Cap_Change_24h,
                    Market_Cap_Change_Percentage_24h, Circulating_Supply,
                    Total_Supply, Ath, Ath_Change_Percentage,
                    DateTime
                ) VALUES %s
                """,
                [tuple(row) for row in df.values],
                page_size=len(df)
            )

            conn.commit()
            print("Datos cargados correctamente")
    except Exception as e:
        print("Error cargando los datos")
        print(e)


