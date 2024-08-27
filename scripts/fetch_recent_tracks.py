import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
import pytz

# Load environment variables from the .env file
load_dotenv()

# Retrieve environment variables
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
    scope="user-read-recently-played user-top-read",
    cache_path=TOKEN_PATH
), requests_timeout=50)

# Set your local timezone
local_timezone = pytz.timezone('Europe/Helsinki')

def fetch_recent_tracks():
    """Fetches the most recent 50 tracks played by the user and saves them to a CSV file without duplicating."""
    LIMIT = 50
    filename = os.path.join(DATA_PATH, "recent_tracks_raw.csv")

    # Fetch the most recent tracks without any time constraints
    try:
        results = sp.current_user_recently_played(limit=LIMIT)
    except spotipy.exceptions.SpotifyException as e:
        print(f"Spotify API error: {e}")
        return

    if os.path.exists(filename):
        # Load existing file and find the most recent played_at
        existing_data = pd.read_csv(filename)
        latest_played_at = existing_data['played_at'].max()
    else:
        latest_played_at = None

    track_data = []
    while results:
        tracks = results.get('items', [])
        if not tracks:
            break

        for item in tracks:
            played_at_utc = pd.to_datetime(item['played_at'])
            played_at_local = played_at_utc.astimezone(local_timezone)
            played_at_formatted = played_at_local.strftime('%Y-%m-%d %H:%M:%S')

            # Skip tracks that are already in the file
            if latest_played_at and played_at_formatted <= latest_played_at:
                continue

            track = item['track']
            primary_artist_id = track['artists'][0]['id']
            artist_info = sp.artist(primary_artist_id)
            genres = artist_info['genres']

            # Check if the track was played from a playlist (context may be None)
            playlist_name = None
            playlist_id = None
            if item['context'] and item['context']['type'] == 'playlist':
                playlist_id = item['context']['uri'].split(':')[-1]  # Extract playlist ID from URI
                playlist_name = sp.playlist(playlist_id)['name']  # Get playlist name via API

            track_data.append({
                'played_at': played_at_formatted,
                'track_name': track['name'],
                'artist_name': ', '.join([artist['name'] for artist in track['artists']]),
                'track_duration_ms': track['duration_ms'],
                'playlist_name': playlist_name,  # Add playlist name (if available)
                'popularity': track['popularity'],
                'artist_genres': ', '.join(genres),

                'album_name': track['album']['name'],
                'album_release_date': track['album']['release_date'],

                'track_id': track['id'],
                'album_id': track['album']['id'],
                'artist_ids': ', '.join([artist['id'] for artist in track['artists']]),
                'playlist_id': playlist_id,  # Add playlist ID (if available)
            })

        if results['next']:
            results = sp.next(results)
        else:
            break

    # Save the new unique data to CSV
    if track_data:
        df = pd.DataFrame(track_data)
        df = df.sort_values(by='played_at', ascending=True)
        if os.path.exists(filename):
            df.to_csv(filename, mode='a', header=False, index=False)
        else:
            df.to_csv(filename, mode='w', header=True, index=False)

        print(f"Recent tracks saved to {filename}")
    else:
        print("No new tracks to save.")

if __name__ == "__main__":
    fetch_recent_tracks()
