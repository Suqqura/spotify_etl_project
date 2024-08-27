import os
import pandas as pd
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth
import spotipy

# Load environment variables from the .env file
load_dotenv()

# Spotify API credentials
CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')
REDIRECT_URI = os.getenv('SPOTIPY_REDIRECT_URI')

# Paths
DATA_PATH = '/home/hh/airflow/dags/spotify_project/data/'
TOKEN_PATH = '/home/hh/airflow/dags/spotify_project/token.txt'

# Create Spotipy client
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope="user-top-read",
    cache_path=TOKEN_PATH
), requests_timeout=50)

def fetch_top_artists():
    """Fetches the user's top artists over different time ranges and saves them to CSV files."""
    ranges = ['short_term', 'medium_term', 'long_term']
    
    for time_range in ranges:
        top_artists_data = []

        try:
            results = sp.current_user_top_artists(limit=50, time_range=time_range)
        except spotipy.exceptions.SpotifyException as e:
            print(f"Spotify API error: {e}")
            return

        for artist in results['items']:
            top_artists_data.append({
                'artist_name': artist['name'],
                'genres': ', '.join(artist['genres']),
                'popularity': artist['popularity'],
                'artist_id': artist['id'],
            })

        df = pd.DataFrame(top_artists_data)

        if not os.path.exists(DATA_PATH):
            os.makedirs(DATA_PATH)

        filename = os.path.join(DATA_PATH, f"top_artists_{time_range}.csv")

        df.to_csv(filename, mode='w', header=True, index=False)

        print(f"Top {time_range} artists saved to {filename}")

if __name__ == "__main__":
    fetch_top_artists()
