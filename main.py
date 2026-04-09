from src.ingest_weather import ingest_once, ensure_indexes
from src.ingest_pg import write_to_postgres

if __name__ == "__main__":
    ensure_indexes()
    clean_data = ingest_once(None)
    # Write clean to pg
    if clean_data:
        write_to_postgres(clean_data)