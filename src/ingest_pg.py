
# Get data from MongoDB, transform it, and write to PostgreSQL

# Columns: city, country, longitude, latitude, temperature, humidity, pressure, wind_speed, wind_direction,
# Observed_at, provider.

# Create a table in PostgreSQL
# Write the clean data to PostgreSQL, ensuring no duplicates based on city, country, and observed_at timestamp.


from pymongo import MongoClient  
import psycopg2 
from psycopg2.extras import execute_values 
from dotenv import load_dotenv  
import os
from datetime import datetime

load_dotenv() # Tells the script to look for that secret .env file.


# MongoDB connection parameters from .env
URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGODB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")


# postgres connection parameters from .env
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")         

_client = None

# get_client() & get_collection(): These functions make sure we have a steady connection to MongoDB.
def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(URI)
    return _client

def get_collection():
    client = get_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return collection

# function to get a connection to PostgreSQL
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT,
            host=PG_HOST,
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None


# write data to postgreSQL 
def write_to_postgres(clean_data: list): 
    print(f"DEBUG: Received {len(clean_data)} records to write to Postgres")
    if not clean_data: 
        print("No data to write to PostgreSQL.")
        return
    for doc in clean_data:
        if isinstance(doc.get("observed_at"), str):
            doc["observed_at"] = datetime.fromisoformat(doc["observed_at"])
            
    conn = get_pg_connection()
    if not conn:
        print("Failed to connect to PostgreSQL")
        return
    cursor = conn.cursor()
    
    # Create table for postgres  
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather (
        id SERIAL PRIMARY KEY,
        city VARCHAR(255),
        country_code VARCHAR(10),
        lat FLOAT,
        lon FLOAT,
        temp FLOAT,
        humidity FLOAT,
        pressure FLOAT,
        wind_speed FLOAT,
        wind_direction FLOAT,
        weather_description TEXT,
        observed_at TIMESTAMP,
        provider VARCHAR(255),
        update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(city, country_code, observed_at) 
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    
    # initialize the list for transformed data
    records = []
    
    # loop through the clean data and transform it into a list of tuples for bulk insertion
    for doc in clean_data:
        # Transform dict into tuple  
        record = (
            doc["city"],
            doc["country_code"],
            doc["coordinates"]["lat"],
            doc["coordinates"]["lon"],
            doc["metrics"]["temperature"],
            doc["metrics"]["humidity"],
            doc["metrics"]["pressure"],
            doc["metrics"]["wind_speed"],
            doc["metrics"]["wind_direction"],
            doc["metrics"]["weather_description"],
            doc["observed_at"],
            doc["provider"]
        )
        records.append(record)
    
    if not records:
        print("No records found after cleaning")
        return
    
    # Insert data with upsert logic
    insert_query = """
    INSERT INTO weather (city, country_code, lat, lon, temp, humidity, 
    pressure, wind_speed, wind_direction, weather_description,
    observed_at, provider)
    VALUES %s
    ON CONFLICT (city, country_code, observed_at) DO UPDATE SET
        lat = EXCLUDED.lat,
        lon = EXCLUDED.lon,
        temp = EXCLUDED.temp,
        humidity = EXCLUDED.humidity,
        pressure = EXCLUDED.pressure,
        wind_speed = EXCLUDED.wind_speed,
        wind_direction = EXCLUDED.wind_direction,
        weather_description = EXCLUDED.weather_description,
        provider = EXCLUDED.provider,
        update_at = CURRENT_TIMESTAMP;
    """
    
    execute_values(cursor, insert_query, records, page_size=1000)
    conn.commit() # The "Signature." It tells the database, "Yes, I am sure about these changes. Save them forever."
    
    cursor.close()
    conn.close()
    