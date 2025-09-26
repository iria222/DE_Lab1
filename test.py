"""
qa_checks.py
Comprobaciones de calidad / integridad para la BD flights_db desde Python.
Requisitos: pandas, sqlalchemy, pymysql
Instalar: pip install pandas sqlalchemy pymysql
"""

import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIG: edita aquí tu cadena de conexión ---
# Ejemplo: "mysql+pymysql://root:mi_pass@127.0.0.1:3306/flights_db"
CONN_STR = "mysql+pymysql://root:password@127.0.0.1:3306/flights_db"

def run_sql(engine, sql, params=None):
    """Ejecuta SQL y devuelve DataFrame."""
    try:
        return pd.read_sql(sql, engine, params=params)
    except Exception as e:
        print("Error ejecutando SQL:", e)
        return None

def main():
    engine = create_engine(CONN_STR)
    print("Conectando a la DB...")

    # 1) Conteos por tabla
    print("\n1) Número de filas por tabla")
    q_counts = """
    SELECT 'airline' AS tabla, COUNT(*) AS filas FROM airline
    UNION ALL
    SELECT 'airport', COUNT(*) FROM airport
    UNION ALL
    SELECT 'date', COUNT(*) FROM date
    UNION ALL
    SELECT 'cancellation_reason', COUNT(*) FROM cancellation_reason
    UNION ALL
    SELECT 'fact_flights', COUNT(*) FROM fact_flights;
    """
    df_counts = run_sql(engine, q_counts)
    print(df_counts.to_string(index=False))

    # 2) Duplicados en dimensiones natural keys
    print("\n2) Duplicados en claves naturales (deberían ser 0 filas cada uno)")
    q_dup_airline = "SELECT airline_iata, COUNT(*) cnt FROM airline GROUP BY airline_iata HAVING cnt > 1;"
    q_dup_airport = "SELECT iata_code, COUNT(*) cnt FROM airport GROUP BY iata_code HAVING cnt > 1;"
    df_dup_a = run_sql(engine, q_dup_airline)
    df_dup_ap = run_sql(engine, q_dup_airport)
    print("\nAirline duplicates:")
    print(df_dup_a if not df_dup_a.empty else "No duplicates found")
    print("\nAirport duplicates:")
    print(df_dup_ap if not df_dup_ap.empty else "No duplicates found")

    # 3) Count vs distinct (sanity)
    print("\n3) Count vs DISTINCT (sanity check)")
    df_airline_counts = run_sql(engine, "SELECT COUNT(*) as total_rows, COUNT(DISTINCT airline_iata) as distinct_keys FROM airline;")
    df_airport_counts = run_sql(engine, "SELECT COUNT(*) as total_rows, COUNT(DISTINCT iata_code) as distinct_keys FROM airport;")
    print("Airline:", df_airline_counts.to_dict(orient='records'))
    print("Airport:", df_airport_counts.to_dict(orient='records'))

    # 4) Orphans (hechos que no referencian dimensiones)
    print("\n4) Orphans en foreign keys (debería ser 0):")
    q_orphan_airline = """
    SELECT COUNT(*) AS orphan_airline FROM fact_flights f
    LEFT JOIN airline a ON f.airline_id = a.airline_id
    WHERE a.airline_id IS NULL;
    """
    q_orphan_origin = """
    SELECT COUNT(*) AS orphan_origin_airport FROM fact_flights f
    LEFT JOIN airport o ON f.origin_airport_id = o.airport_id
    WHERE o.airport_id IS NULL;
    """
    q_orphan_dest = """
    SELECT COUNT(*) AS orphan_dest_airport FROM fact_flights f
    LEFT JOIN airport d ON f.destination_airport_id = d.airport_id
    WHERE d.airport_id IS NULL;
    """
    q_orphan_date = """
    SELECT COUNT(*) AS orphan_date FROM fact_flights f
    LEFT JOIN date dt ON f.date_id = dt.date_id
    WHERE dt.date_id IS NULL;
    """
    for q in (q_orphan_airline, q_orphan_origin, q_orphan_dest, q_orphan_date):
        print(run_sql(engine, q).to_string(index=False))

    # Si alguno > 0, mostrar ejemplos
    orphans_df = run_sql(engine, """
    SELECT f.flight_id, f.airline_id, f.origin_airport_id, f.destination_airport_id, f.date_id
    FROM fact_flights f
    LEFT JOIN airline a ON f.airline_id = a.airline_id
    LEFT JOIN airport o ON f.origin_airport_id = o.airport_id
    LEFT JOIN airport d ON f.destination_airport_id = d.airport_id
    LEFT JOIN date dt ON f.date_id = dt.date_id
    WHERE a.airline_id IS NULL OR o.airport_id IS NULL OR d.airport_id IS NULL OR dt.date_id IS NULL
    LIMIT 30;
    """)
    if orphans_df is not None and not orphans_df.empty:
        print("\nEjemplos de filas huérfanas (max 30):")
        print(orphans_df.to_string(index=False))
    else:
        print("\nNo se encontraron filas huérfanas en sample (0 o ninguna muestra).")

    # 5) Nulls en columnas clave del hecho
    print("\n5) Nulls en claves del hecho (deberían ser 0 o muy pocos según tu diseño):")
    q_nulls = """
    SELECT 
      SUM(CASE WHEN airline_id IS NULL THEN 1 ELSE 0 END) AS null_airline_id,
      SUM(CASE WHEN origin_airport_id IS NULL THEN 1 ELSE 0 END) AS null_origin_airport_id,
      SUM(CASE WHEN destination_airport_id IS NULL THEN 1 ELSE 0 END) AS null_destination_airport_id,
      SUM(CASE WHEN date_id IS NULL THEN 1 ELSE 0 END) AS null_date_id
    FROM fact_flights;
    """
    print(run_sql(engine, q_nulls).to_string(index=False))

    # 6) Spot-check de vuelos con dimensiones
    print("\n6) Muestra sample de fact_flights con joins a dimensiones (30 filas)")
    q_sample = """
    SELECT f.flight_id, f.flight_number, a.airline_iata, a.airline_name,
           o.iata_code AS origin_iata, d.iata_code AS dest_iata,
           f.scheduled_departure, f.departure_time, f.arrival_delay
    FROM fact_flights f
    LEFT JOIN airline a ON f.airline_id = a.airline_id
    LEFT JOIN airport o ON f.origin_airport_id = o.airport_id
    LEFT JOIN airport d ON f.destination_airport_id = d.airport_id
    LIMIT 30;
    """
    df_sample = run_sql(engine, q_sample)
    print(df_sample.to_string(index=False))

    # 7) Stats de columnas numéricas y anomalías básicas
    print("\n7) Estadísticas rápidas y anomalías")
    print("Arrival delay stats:")
    print(run_sql(engine, "SELECT MIN(arrival_delay) AS min_delay, AVG(arrival_delay) AS avg_delay, MAX(arrival_delay) AS max_delay FROM fact_flights;").to_string(index=False))
    print("Distance <= 0 count:", run_sql(engine, "SELECT COUNT(*) AS negative_distance_count FROM fact_flights WHERE distance <= 0;").to_string(index=False))
    print("Null air_time count:", run_sql(engine, "SELECT COUNT(*) AS null_air_time FROM fact_flights WHERE air_time IS NULL;").to_string(index=False))

    # 8) Cancelaciones y reason
    print("\n8) Comprobación cancelaciones")
    print(run_sql(engine, "SELECT COUNT(*) AS cancelled_with_no_reason FROM fact_flights WHERE is_cancelled = 1 AND (cancellation_id IS NULL OR cancellation_id = 0);").to_string(index=False))
    print(run_sql(engine, "SELECT COUNT(*) AS cancelled_with_reason FROM fact_flights WHERE is_cancelled = 1 AND cancellation_id IS NOT NULL;").to_string(index=False))

    print("\nQA checks finalizados.")

if __name__ == "__main__":
    main()
