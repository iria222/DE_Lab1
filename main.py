import pandas as pd
from sqlalchemy import create_engine
from data_preparation import *
import json
import gc
import time
import math
import numpy as np



def load_db_config(config_file='config.json'):
    with open(config_file, 'r') as file:
        return json.load(file)
    
def get_connection(db_config):
    return create_engine(
        f'mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}'
    )


def load_airline_data(engine, airlines_data):
    try:
        airlines_data.to_sql('airline', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading airlines dimension table: \n", ex)
    else:
        print("Airlines dimension table loaded successfully.")


def load_airport_data(engine, airports_data):
    try:
        airports_data.to_sql('airport', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading airports dimension table: \n", ex)
    else:
        print("Airports dimension table loaded successfully.")


def load_date_data(engine, date_data):
    try:
        date_data.to_sql('date', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading date dimension table: \n", ex)
    else:
        print("Date dimension table loaded successfully.")


def load_cancellation_data(engine, cancellation_data):
    try:
        cancellation_data.to_sql('cancellation_reason', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading cancellation dimension table: \n", ex)
    else:
        print("Cancellation dimension table loaded successfully.")


def load_flight_data(engine, flight_data):
    try:
        flight_data.to_sql('fact_flights', con=engine, if_exists='append', index=False, chunksize=50000, method='multi')
    except Exception as ex:
        print("Error while loading flight fact table: \n", ex)
    else:
        print("Flight fact table loaded successfully.")


def _to_native_scalar(v):
   
    if pd.isna(v):
        return None
    if isinstance(v, (np.generic,)):
        try:
            return v.item()
        except Exception:
            return v
    return v

def load_flight_data(engine, flight_data, chunksize=2000, disable_fk=False):
 
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        if disable_fk:
            cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
            conn.commit()

        cols = list(flight_data.columns)
        placeholders = ",".join(["%s"] * len(cols))
        col_list_sql = ",".join([f"`{c}`" for c in cols])
        insert_sql = f"INSERT INTO fact_flights ({col_list_sql}) VALUES ({placeholders})"

        total = len(flight_data)
        batches = math.ceil(total / chunksize)
        # print(f"Inserting {total} rows in {batches} batches of {chunksize}...")

        for i in range(batches):
            start = i * chunksize
            end = min(start + chunksize, total)
            chunk = flight_data.iloc[start:end]

            to_list = []
            for row in chunk.itertuples(index=False, name=None):
                new_row = tuple(_to_native_scalar(v) for v in row)
                to_list.append(new_row)

            t0 = time.time()
            try:
                cursor.executemany(insert_sql, to_list)
                conn.commit()
                print(f"  -> Batch {i+1}/{batches} inserted rows {start+1}-{end} in {time.time()-t0:.1f}s")
            except Exception as ex:
                conn.rollback()
                print(f"Error inserting batch {i+1}: {ex}")

                # print("Sample converted rows (first 5):")
                for r in to_list[:5]:
                    print(r)
                raise
            finally:
                # liberar memoria
                del chunk
                del to_list
                gc.collect()

        if disable_fk:
            cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
            conn.commit()

        print("All batches inserted successfully.")
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    
    airlines_data = pd.read_csv("Data/airlines.csv")
    airports_data = pd.read_csv("Data/airports.csv")
    flights_data = pd.read_csv("Data/flights.csv")

    tmp = pd.read_csv("Data/flights.csv", nrows=0)
    print(tmp.columns.tolist()[7], tmp.columns.tolist()[8])

    try:
        # Get the connection with the database
        db_config = load_db_config()
        engine = get_connection(db_config)
        print(f"Connection to the {db_config['host']} for user {db_config['user']} created successfully.")
        
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)

    load_airline_data(engine, prepare_airline_data(airlines_data))
    load_airport_data(engine, prepare_airport_data(airports_data))
    load_date_data(engine, prepare_date_data(flights_data))
    load_cancellation_data(engine, prepare_cancellation_data(flights_data))



    # Read the dimension tables to get the IDs and add them to the fact table
    airport_db = pd.read_sql('SELECT airport_id, iata_code FROM airport', con=engine)
    airline_db = pd.read_sql('SELECT airline_id, airline_iata FROM airline', con=engine)
    date_db = pd.read_sql('SELECT date_id, year, month, day FROM date', con=engine)
    cancellation_db = pd.read_sql('SELECT cancellation_id, cancellation_type FROM cancellation_reason', con=engine)

    facts_flight_data = prepare_flight_data(flights_data, cancellation_db, date_db, airport_db, airline_db)

    # print("facts_flight_data shape:", facts_flight_data.shape)

    # print("Memoria aproximada (MB):", facts_flight_data.memory_usage(deep=True).sum() / 1024**2)


    # DEBUG: inspección rápida del DataFrame de hechos antes de insertar
    # print(">>> DEBUG: facts_flight_data tipo:", type(facts_flight_data))
    # print(">>> DEBUG: facts_flight_data shape:", getattr(facts_flight_data, "shape", None))
    # try:
    #     print(">>> DEBUG: primeras filas:\n", facts_flight_data.head(5))
    #     print(">>> DEBUG: info():")
    #     facts_flight_data.info()
    #     print(">>> DEBUG: nulos por columna:\n", facts_flight_data.isnull().sum().sort_values(ascending=False).head(20))
    # except Exception as e:
    #     print(">>> DEBUG: error mostrando facts_flight_data:", e)

    # # Chequeos de mapeo (muy útiles si prepare_flight_data hace merges/mappings)
    # # (usa flights_data original para comparar valores sin mapear)
    # print("Distinct ORIGIN_AIRPORT sample (first 20):", flights_data['ORIGIN_AIRPORT'].dropna().astype(str).unique()[:20])
    # print("Distinct DESTINATION_AIRPORT sample (first 20):", flights_data['DESTINATION_AIRPORT'].dropna().astype(str).unique()[:20])

    # # Si existen columnas origin_airport_id / destination_airport_id, contar nulos
    # if 'origin_airport_id' in facts_flight_data.columns:
    #     print("origin_airport_id nulls:", facts_flight_data['origin_airport_id'].isnull().sum())
    # if 'destination_airport_id' in facts_flight_data.columns:
    #     print("destination_airport_id nulls:", facts_flight_data['destination_airport_id'].isnull().sum())
    # if 'airline_id' in facts_flight_data.columns:
    #     print("airline_id nulls:", facts_flight_data['airline_id'].isnull().sum())
    # if 'date_id' in facts_flight_data.columns:
    #     print("date_id nulls:", facts_flight_data['date_id'].isnull().sum())

    # load_flight_data(engine,facts_flight_data )

    load_flight_data(engine, facts_flight_data, chunksize=2000, disable_fk=True)



