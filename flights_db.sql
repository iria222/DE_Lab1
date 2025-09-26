
CREATE DATABASE IF NOT EXISTS flights_db;
USE flights_db;

-- Dimensiones simples (airlines, airports)
CREATE TABLE IF NOT EXISTS airline (
  airline_id INT AUTO_INCREMENT PRIMARY KEY,
  airline_iata VARCHAR(10) UNIQUE NOT NULL,
  airline_name VARCHAR(255)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS airport (
  airport_id INT AUTO_INCREMENT PRIMARY KEY,
  iata_code VARCHAR(10) UNIQUE NOT NULL,
  airport_name VARCHAR(255),
  city VARCHAR(100),
  state VARCHAR(50),
  country VARCHAR(50),
  latitude DOUBLE,
  longitude DOUBLE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS date (
  date_id INT AUTO_INCREMENT PRIMARY KEY,
  year INT,
  month INT,
  day INT,
  day_of_week VARCHAR(20)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS cancellation_reason (
  cancellation_id INT AUTO_INCREMENT PRIMARY KEY,
  cancellation_type VARCHAR(100)
) ENGINE=InnoDB;

-- Tabla de hechos (fact_flights) con las columnas que el script Python escribe (df_db)
CREATE TABLE IF NOT EXISTS fact_flights (
  flight_id INT AUTO_INCREMENT PRIMARY KEY,
  flight_number INT,
  aircraft_id VARCHAR(100),
  airline_id INT,
  origin_airport_id INT,
  destination_airport_id INT,
  date_id INT,
  cancellation_id INT,
  scheduled_departure TIME,
  scheduled_time INT,
  departure_time TIME,
  departure_delay INT,
  taxi_out INT,
  wheels_off TIME,
  elapsed_time INT,
  air_time INT,
  distance INT,
  wheels_on TIME,
  taxi_in INT,
  scheduled_arrival TIME,
  arrival_time TIME,
  arrival_delay INT,
  is_diverted BOOL,
  is_cancelled BOOL,
  
  foreign key(airline_id) references airline(airline_id),
  foreign key(origin_airport_id) references airport(airport_id),
  foreign key(destination_airport_id) references airport(airport_id),
  foreign key(date_id) references date(date_id),
  foreign key(cancellation_id) references cancellation_reason(cancellation_id)
) ENGINE=InnoDB;
