import pandas as pd
from sqlalchemy import create_engine
from data_preparation import *
import json

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
        flight_data.to_sql('fact_flights', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading flight fact table: \n", ex)
    else:
        print("Flight fact table loaded successfully.")

if __name__ == '__main__':
    
    airlines_data = pd.read_csv("Data/airlines.csv")
    airports_data = pd.read_csv("Data/airports.csv")
    flights_data = pd.read_csv("Data/flights.csv")

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
    load_flight_data(engine,prepare_flight_data(flights_data, engine))
