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

def fetch_top_tracks():
    """Fetches the user's top tracks over different time ranges and saves them to CSV files."""
    ranges = ['short_term', 'medium_term', 'long_term']
    
    for time_range in ranges:
        top_tracks_data = []
        try:
            results = sp.current_user_top_tracks(limit=50, time_range=time_range)
        except spotipy.exceptions.SpotifyException as e:
            print(f"Spotify API error: {e}")
            return

        for item in results['items']:
            track = item
            primary_artist_id = track['artists'][0]['id']
            artist_info = sp.artist(primary_artist_id)
            genres = artist_info['genres']

            top_tracks_data.append({
                'track_name': track['name'],
                'artist_name': ', '.join([artist['name'] for artist in track['artists']]),
                'track_duration_ms': track['duration_ms'],
                'popularity': track['popularity'],
                'artist_genres': ', '.join(genres),

                'album_name': track['album']['name'],
                'album_release_date': track['album']['release_date'],

                'track_id': track['id'],
                'album_id': track['album']['id'],
                'artist_ids': ', '.join([artist['id'] for artist in track['artists']]),
            })

        df = pd.DataFrame(top_tracks_data)

        if not os.path.exists(DATA_PATH):
            os.makedirs(DATA_PATH)

        filename = os.path.join(DATA_PATH, f"top_tracks_{time_range}.csv")

        df.to_csv(filename, mode='w', header=True, index=False)

        print(f"Top {time_range} tracks saved to {filename}")

if __name__ == "__main__":
    fetch_top_tracks()
