import pandas as pd
import datetime
import time as _time
import numpy as np


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

def _log(msg):
    print(f"[prepare_flight_data] {msg}")

def _format_time_series(ser):

    s = ser.astype(str).str.extract(r'(\d{1,4})', expand=False).fillna('')
    s = s.str.zfill(4)  # '5'->'0005', '930'->'0930'
    s = s.where(s != '', pd.NA)

    formatted = pd.Series(pd.NA, index=ser.index, dtype="object")
    mask = s.notna()
    formatted.loc[mask] = s.loc[mask].str.slice(0,2) + ':' + s.loc[mask].str.slice(2,4) + ':00'
    return formatted

def prepare_flight_data(flight_data, cancellation_db, date_db, airport_db, airline_db):
    t0 = _time.time()
    _log("start (no-merge version)")

    cols_maybe_needed = [
        'FLIGHT_NUMBER','TAIL_NUMBER','AIRLINE','ORIGIN_AIRPORT','DESTINATION_AIRPORT',
        'YEAR','MONTH','DAY','CANCELLATION_REASON',
        'SCHEDULED_DEPARTURE','SCHEDULED_TIME','DEPARTURE_TIME','DEPARTURE_DELAY',
        'TAXI_OUT','WHEELS_OFF','ELAPSED_TIME','AIR_TIME','DISTANCE','WHEELS_ON','TAXI_IN',
        'SCHEDULED_ARRIVAL','ARRIVAL_TIME','ARRIVAL_DELAY','DIVERTED','CANCELLED'
    ]
    present_cols = [c for c in cols_maybe_needed if c in flight_data.columns]
    df = flight_data[present_cols].copy()

    time_columns = ['SCHEDULED_DEPARTURE','DEPARTURE_TIME','WHEELS_OFF','WHEELS_ON','SCHEDULED_ARRIVAL','ARRIVAL_TIME']
    for col in time_columns:
        if col in df.columns:
            df[col] = _format_time_series(df[col])
    _log(f"time cols formatted in {_time.time()-t0:.2f}s")


    date_map = {}
    if {'date_id','year','month','day'}.issubset(date_db.columns):

        date_map = { f"{int(r.year)}-{int(r.month)}-{int(r.day)}": int(r.date_id)
                     for _, r in date_db.iterrows() }
    else:
        _log("WARNING: date_db no contiene date_id/year/month/day")

    airport_map = {}
    if {'iata_code','airport_id'}.issubset(airport_db.columns):
        airport_map = dict(zip(airport_db['iata_code'].astype(str).str.upper().str.strip(),
                               airport_db['airport_id'].astype(int)))
    else:
        _log("WARNING: airport_db no contiene iata_code/airport_id")

    airline_map = {}
    if {'airline_iata','airline_id'}.issubset(airline_db.columns):
        airline_map = dict(zip(airline_db['airline_iata'].astype(str).str.upper().str.strip(),
                               airline_db['airline_id'].astype(int)))
    else:
        _log("WARNING: airline_db no contiene airline_iata/airline_id")

    cancellation_map = {}
    if {'cancellation_id','cancellation_type'}.issubset(cancellation_db.columns):
        cancellation_map = dict(zip(cancellation_db['cancellation_type'].astype(str),
                                    cancellation_db['cancellation_id'].astype(int)))
    else:
        _log("WARNING: cancellation_db no contiene cancellation_type/cancellation_id")


    if {'YEAR','MONTH','DAY'}.issubset(df.columns) and date_map:
        date_key = df['YEAR'].astype(str).str.strip() + '-' + df['MONTH'].astype(str).str.strip() + '-' + df['DAY'].astype(str).str.strip()
        df['date_id'] = date_key.map(date_map)
    else:
        df['date_id'] = pd.NA
    _log(f"date_id mapped in {_time.time()-t0:.2f}s")


    if 'ORIGIN_AIRPORT' in df.columns:
        df['origin_airport_id'] = df['ORIGIN_AIRPORT'].astype(str).str.extract(r'([A-Za-z0-9]+)', expand=False).fillna('').str.upper().map(airport_map)
    else:
        df['origin_airport_id'] = pd.NA

    if 'DESTINATION_AIRPORT' in df.columns:
        df['destination_airport_id'] = df['DESTINATION_AIRPORT'].astype(str).str.extract(r'([A-Za-z0-9]+)', expand=False).fillna('').str.upper().map(airport_map)
    else:
        df['destination_airport_id'] = pd.NA

    if 'AIRLINE' in df.columns:
        df['airline_id'] = df['AIRLINE'].astype(str).str.extract(r'([A-Za-z0-9]+)', expand=False).fillna('').str.upper().map(airline_map)
    else:
        df['airline_id'] = pd.NA

    if 'CANCELLATION_REASON' in df.columns:
        df['cancellation_id'] = df['CANCELLATION_REASON'].map(cancellation_map)
    else:
        df['cancellation_id'] = pd.NA

    _log(f"airports/airline/cancellation mapped in {_time.time()-t0:.2f}s")

    num_cols = ['SCHEDULED_TIME','DEPARTURE_DELAY','TAXI_OUT','ELAPSED_TIME','AIR_TIME','DISTANCE','TAXI_IN','ARRIVAL_DELAY','FLIGHT_NUMBER']
    for nc in num_cols:
        if nc in df.columns:
            df[nc] = pd.to_numeric(df[nc], errors='coerce')

    if 'DISTANCE' in df.columns:
        df['DISTANCE'] = pd.to_numeric(df['DISTANCE'], errors='coerce').astype('Float32')
    if 'AIR_TIME' in df.columns:
        df['AIR_TIME'] = pd.to_numeric(df['AIR_TIME'], errors='coerce').astype('Float32')
    if 'FLIGHT_NUMBER' in df.columns:
        df['FLIGHT_NUMBER'] = pd.to_numeric(df['FLIGHT_NUMBER'], errors='coerce').astype('Int32')


    rename_map = {
        'FLIGHT_NUMBER':'flight_number',
        'TAIL_NUMBER':'aircraft_id',
        'SCHEDULED_DEPARTURE':'scheduled_departure',
        'SCHEDULED_TIME':'scheduled_time',
        'DEPARTURE_TIME':'departure_time',
        'DEPARTURE_DELAY':'departure_delay',
        'TAXI_OUT':'taxi_out',
        'WHEELS_OFF':'wheels_off',
        'ELAPSED_TIME':'elapsed_time',
        'AIR_TIME':'air_time',
        'DISTANCE':'distance',
        'WHEELS_ON':'wheels_on',
        'TAXI_IN':'taxi_in',
        'SCHEDULED_ARRIVAL':'scheduled_arrival',
        'ARRIVAL_TIME':'arrival_time',
        'ARRIVAL_DELAY':'arrival_delay',
        'DIVERTED':'is_diverted',
        'CANCELLED':'is_cancelled'
    }

    df = df.rename(columns=rename_map)


    needed_columns = ['flight_number','aircraft_id','airline_id','origin_airport_id','destination_airport_id',
                      'date_id','cancellation_id','scheduled_departure','scheduled_time','departure_time','departure_delay',
                      'taxi_out','wheels_off','elapsed_time','air_time','distance','wheels_on','taxi_in',
                      'scheduled_arrival','arrival_time','arrival_delay','is_diverted','is_cancelled']

    existing_final_cols = [c for c in needed_columns if c in df.columns]
    result = df[existing_final_cols].copy()

    show_null_summary(result, ['scheduled_time','cancellation_id','origin_airport_id','destination_airport_id','air_time','arrival_delay'])
    result = drop_missing_scheduled_time(result, save_dropped=True, dropped_path='dropped_missing_scheduled_time.csv')

    result = postprocess_cancellations(result, cancellation_db=None, add_unknown=False)
    show_null_summary(result, ['scheduled_time','cancellation_id','origin_airport_id','destination_airport_id','air_time','arrival_delay'])

    _log(f"done in {_time.time()-t0:.2f}s, result shape: {result.shape}")
    return result

def drop_missing_scheduled_time(df, save_dropped=True, dropped_path='dropped_missing_scheduled_time.csv'):
  
    import os
    total = len(df)
    if 'scheduled_time' not in df.columns:
        return df.copy()

    missing = df['scheduled_time'].isna().sum()
    print(f"[drop_missing_scheduled_time] total rows before = {total}, missing scheduled_time = {missing}")

    if missing == 0:
        return df.copy()

   
    kept_df = df[df['scheduled_time'].notna()].copy()

    return kept_df

    

def postprocess_cancellations(df, cancellation_db=None, add_unknown=False):
    """
    Asegura coherencia entre is_cancelled y cancellation_id:
    - si is_cancelled == 0 => cancellation_id = NULL
    - si is_cancelled == 1 y cancellation_id es NULL => si add_unknown=True crea/mapea 'Unknown' (se necesita cancellation_db mutable fuera)
    - si cancellation_db no se pasa, no crea ids
    """
    # Normalizar is_cancelled a 0/1 ints
    if 'is_cancelled' in df.columns:
        df['is_cancelled'] = pd.to_numeric(df['is_cancelled'], errors='coerce').fillna(0).astype(int)

    # Si hay filas no canceladas, asegurarse cancellation_id es NULL
    if 'cancellation_id' in df.columns:
        df.loc[df['is_cancelled'] == 0, 'cancellation_id'] = pd.NA

        # Si se pide, mapear valores faltantes de cancel_id a un "Unknown" en cancellation_db
        if add_unknown and cancellation_db is not None:
            # comprobar si ya existe cancellation_type 'Unknown' (o 'OTHER')
            unk = cancellation_db[cancellation_db['cancellation_type'].str.lower().isin(['unknown','other','not specified'])]
            if len(unk) > 0:
                unk_id = int(unk.iloc[0]['cancellation_id'])
            else:
                print("[postprocess_cancellations] WARNING: add_unknown=True pero no se insertó 'Unknown' en cancellation_db automáticamente. Inserta manualmente en la dimensión o pásala al cargar.")
                unk_id = None
            if unk_id is not None:
                df.loc[(df['is_cancelled'] == 1) & (df['cancellation_id'].isna()), 'cancellation_id'] = int(unk_id)

    return df

def show_null_summary(df, cols=None):
    """Imprime resumen de nulos por columna (top) y total filas."""
    total = len(df)
    print(f"[null summary] total rows = {total}")
    if cols is None:
        cols = df.columns.tolist()
    nulls = df[cols].isnull().sum().sort_values(ascending=False)
    print(nulls[nulls > 0].to_string())

