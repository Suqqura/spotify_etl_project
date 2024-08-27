import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql

# Load environment variables from the .env file
load_dotenv()

# Database connection settings
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Path to the transformed data
DATA_PATH = '/home/hh/airflow/dags/spotify_project/data/'
TRANSFORMED_FILE = os.path.join(DATA_PATH, "recent_tracks.csv")

def create_table_if_not_exists(conn):
    """Creates the 'recent_tracks' table if it does not exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS recent_tracks (
        played_at TIMESTAMP PRIMARY KEY,
        track_name TEXT,
        artist_name TEXT,
        track_duration_ms FLOAT,
        popularity INTEGER,
        artist_genres TEXT,
        album_name TEXT,
        album_release_date TEXT,
        album_id TEXT,
        track_id TEXT,
        artist_ids TEXT,
        playlist_name TEXT,
        playlist_id TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()

def load_data_to_db():
    """Loads the transformed data into the database."""
    # Load the transformed data
    if not os.path.exists(TRANSFORMED_FILE):
        print(f"No transformed data found at {TRANSFORMED_FILE}")
        return

    df = pd.read_csv(TRANSFORMED_FILE)

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    try:
        # Create the table if it doesn't exist
        create_table_if_not_exists(conn)

        # Load the data into the database
        with conn.cursor() as cur:
            for index, row in df.iterrows():
                insert_query = sql.SQL("""
                INSERT INTO recent_tracks (
                    played_at, track_name, artist_name, track_duration_ms, popularity, artist_genres, 
                    album_name, album_release_date, album_id, track_id, artist_ids, playlist_name, playlist_id
                ) VALUES (
                    {played_at}, {track_name}, {artist_name}, {track_duration_ms}, {popularity}, {artist_genres}, 
                    {album_name}, {album_release_date}, {album_id}, {track_id}, {artist_ids}, {playlist_name}, {playlist_id}
                ) ON CONFLICT (played_at) DO NOTHING;
                """).format(
                    played_at=sql.Literal(row['played_at']),
                    track_name=sql.Literal(row['track_name']),
                    artist_name=sql.Literal(row['artist_name']),
                    track_duration_ms=sql.Literal(row['track_duration_ms']),
                    popularity=sql.Literal(row['popularity']),
                    artist_genres=sql.Literal(row['artist_genres']),
                    album_name=sql.Literal(row['album_name']),
                    album_release_date=sql.Literal(row['album_release_date']),
                    album_id=sql.Literal(row['album_id']),
                    track_id=sql.Literal(row['track_id']),
                    artist_ids=sql.Literal(row['artist_ids']),
                    playlist_name=sql.Literal(row['playlist_name']),
                    playlist_id=sql.Literal(row['playlist_id']),
                )
                cur.execute(insert_query)
            conn.commit()
        print("Data successfully loaded into the database.")
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    load_data_to_db()
