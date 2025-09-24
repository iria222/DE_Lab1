import pandas as pd
from sqlalchemy import create_engine
import json

def load_db_config(config_file='config.json'):
    with open(config_file, 'r') as file:
        return json.load(file)
    
def get_connection(db_config):
    return create_engine(
        f'mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}'
    )


def prepare_airline_data(airlines_data):
    new_airlines_data = airlines_data[['IATA_CODE', 'AIRLINE']].drop_duplicates().copy()
    new_airlines_data = new_airlines_data.rename(
        columns={
            'IATA_CODE' : 'airline_iata',
            'AIRLINE' : 'airline_name'
        }
    )
    return new_airlines_data

def prepare_airport_data(airports_data):
    new_airports_data = airports_data[['IATA_CODE', 'AIRPORT', 'CITY', 'STATE', 'COUNTRY', 'LATITUDE', 'LONGITUDE']].drop_duplicates().copy()
    new_airports_data = new_airports_data.rename(
        columns={
            'IATA_CODE' : 'iata_code',
            'AIRPORT' : 'airport_name',
            'CITY' : 'city',
            'STATE' : 'state',
            'COUNTRY' : 'country',
            'LATITUDE' : 'latitude',
            'LONGITUDE' : 'longitude'
        }
    )
    return new_airports_data

def prepare_date_data(flights_data):
    new_date_data = flights_data[['YEAR', 'MONTH', 'DAY', 'DAY_OF_WEEK']].drop_duplicates().copy()
    new_date_data = new_date_data.rename(
        columns={
            'YEAR' : 'year',
            'MONTH' : 'month',
            'DAY' : 'day',
            'DAY_OF_WEEK' : 'day_of_week'
        }
    )
    days = {1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday', 5: 'Friday', 6: 'Saturday', 7: 'Sunday'}
    new_date_data['day_of_week'] = new_date_data['day_of_week'].map(days)
    return new_date_data

def prepare_cancellation_data(flights_data):
    new_cancellation_data = flights_data[['CANCELLATION_REASON']].drop_duplicates().dropna().copy()
    new_cancellation_data = new_cancellation_data.rename(
        columns={
            'CANCELLATION_REASON' : 'cancellation_type'
        }
    )

    return new_cancellation_data

def prepare_flight_data(flights_data):
    #TODO: No copiar. Trabajar sobre la data inicial y luego seleccionar las columnas necesarias
    #TODO: lidiar con nulos
    new_flight_data = flights_data[['FLIGHT_NUMBER', 'TAIL_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'DEPARTURE_DELAY', 'TAXI_OUT', 'WHEELS_OFF', 'SCHEDULED_TIME', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE', 'WHEELS_ON', 'TAXI_IN', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME', 'ARRIVAL_DELAY', 'DIVERTED', 'CANCELLED', 'CANCELLATION_CODE']].copy()
    new_flight_data['SCHEDULED_DEPARTURE'] = pd.to_datetime(new_flight_data['SCHEDULED_DEPARTURE'], format='%H%M').dt.time
    new_flight_data['DEPARTURE_TIME'] = pd.to_datetime(new_flight_data['DEPARTURE_TIME'], format='%H%M').dt.time
    new_flight_data['DEPARTURE_DELAY'] = pd.to_datetime(new_flight_data['DEPARTURE_DELAY'], format='%H%M').dt.time
    new_flight_data['SCHEDULED_TIME'] = pd.to_datetime(new_flight_data['SCHEDULED_TIME'], format='%H%M').dt.time
    new_flight_data['WHEELS_OFF'] = pd.to_datetime(new_flight_data['WHEELS_OFF'], format='%H%M').dt.time
    new_flight_data['WHEELS_ON'] = pd.to_datetime(new_flight_data['WHEELS_ON'], format='%H%M').dt.time
    new_flight_data['SCHEDULED_ARRIVAL'] = pd.to_datetime(new_flight_data['SCHEDULED_ARRIVAL'], format='%H%M').dt.time
    new_flight_data['ARRIVAL_TIME'] = pd.to_datetime(new_flight_data['ARRIVAL_TIME'], format='%H%M').dt.time
    
    new_flight_data = new_flight_data.rename(
        columns={
            'AIRLINE' : 'airline_id',
            'FLIGHT_NUMBER' : 'flight_id',
            'TAIL_NUMBER' : 'aircraft_id',
            'ORIGIN_AIRPORT' : 'origin_airport_id',
            'DESTINATION_AIRPORT' : 'destination_airport_id',
            'SCHEDULED_DEPARTURE' : 'scheduled_departure',
            'DEPARTURE_TIME' : 'departure_time',
            'DEPARTURE_DELAY' : 'departure_delay',
            'TAXI_OUT' : 'taxi_out',
            'WHEELS_OFF' : 'wheels_off',
            'SCHEDULED_TIME' : 'scheduled_time',
            'ELAPSED_TIME' : 'elapsed_time',
            'AIR_TIME' : 'air_time',
            'DISTANCE' : 'distance',
            'WHEELS_ON' : 'wheels_on',
            'TAXI_IN' : 'taxi_in',
            'SCHEDULED_ARRIVAL' : 'scheduled_arrival',
            'ARRIVAL_TIME' : 'arrival_time',
            'ARRIVAL_DELAY' : 'arrival_delay',
            'DIVERTED' : 'diverted',
            'CANCELLED' : 'cancelled',
            'CANCELLATION_CODE' : 'cancellation_code'
        }
    )
    
    return new_flight_data


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
