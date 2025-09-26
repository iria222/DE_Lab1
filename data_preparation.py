import pandas as pd

def prepare_airline_data(airlines_data):
    airlines_data = airlines_data.rename(
        columns={
            'IATA_CODE' : 'airline_iata',
            'AIRLINE' : 'airline_name'
        }
    )
    return airlines_data[['airline_iata', 'airline_name']].drop_duplicates()

def prepare_airport_data(airports_data):
    airports_data = airports_data.rename(
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
    return airports_data[['iata_code', 'airport_name', 'city', 'state', 'country', 'latitude', 'longitude']].drop_duplicates()

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

def prepare_flight_data(flight_data, cancellation_db, date_db, airport_db, airline_db):
    
    #Change time values to proper format
    time_columns = ['SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'WHEELS_OFF', 
                    'WHEELS_ON', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME']
    for col in time_columns:
        flight_data[col] = pd.to_datetime(flight_data[col], format = '%H%M', errors='coerce').dt.strftime('%H:%M:%S')
    
    # Add date ID to fact table
    new_flight_data = flight_data.merge(
        date_db,
        how='left',
        left_on=['YEAR', 'MONTH', 'DAY'],
        right_on=['year', 'month', 'day']
    ).drop(columns=['YEAR', 'MONTH', 'DAY', 'year', 'month', 'day'])

    #Add cancellation ID to fact table
    cancellation_map = dict(zip(cancellation_db['cancellation_type'], cancellation_db['cancellation_id']))
    new_flight_data['cancellation_id'] = flight_data['CANCELLATION_REASON'].map(cancellation_map)

    # Add origin airport ID to fact table
    origin_map = dict(zip(airport_db['iata_code'], airport_db['airport_id']))
    new_flight_data['origin_airport_id'] = flight_data['ORIGIN_AIRPORT'].map(origin_map)

    new_flight_data= new_flight_data.rename(columns={'airport_id': 'origin_airport_id'})
    
    # Add destination airport ID to fact table
    dest_map = dict(zip(airport_db['iata_code'], airport_db['airport_id']))
    new_flight_data['destination_airport_id'] = flight_data['DESTINATION_AIRPORT'].map(dest_map)
    new_flight_data= new_flight_data.rename(columns={'airport_id': 'destination_airport_id'})

    # Add airline ID to fact table
    airline_map = dict(zip(airline_db['airline_iata'], airline_db['airline_id']))
    new_flight_data['airline_id'] = flight_data['AIRLINE'].map(airline_map)

    new_flight_data = new_flight_data.rename(
        columns = {
            'FLIGHT_NUMBER': 'flight_number',
            'TAIL_NUMBER': 'aircraft_id',
            'SCHEDULED_DEPARTURE': 'scheduled_departure',
            'SCHEDULED_TIME': 'scheduled_time',
            'DEPARTURE_TIME': 'departure_time',
            'DEPARTURE_DELAY': 'departure_delay',
            'TAXI_OUT': 'taxi_out',
            'WHEELS_OFF': 'wheels_off',
            'ELAPSED_TIME': 'elapsed_time',
            'AIR_TIME': 'air_time',
            'DISTANCE': 'distance',
            'WHEELS_ON': 'wheels_on',
            'TAXI_IN': 'taxi_in',
            'SCHEDULED_ARRIVAL': 'scheduled_arrival',
            'ARRIVAL_TIME':'arrival_time',
            'ARRIVAL_DELAY':'arrival_delay',
            'DIVERTED': 'is_diverted',
            'CANCELLED':'is_cancelled'
        }
    )

    needed_columns = ['flight_number','aircraft_id', 'airline_id', 'origin_airport_id', 'destination_airport_id', 'date_id', 'cancellation_id',
                      'scheduled_departure', 'scheduled_time', 'departure_time', 'departure_delay', 'taxi_out', 'wheels_off', 'elapsed_time', 
                      'air_time', 'distance', 'wheels_on', 'taxi_in', 'scheduled_arrival', 'arrival_time', 'arrival_delay', 'is_diverted', 'is_cancelled']

    return new_flight_data[needed_columns]