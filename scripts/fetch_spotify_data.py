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
), requests_timeout=10)

# Set your local timezone
local_timezone = pytz.timezone('Europe/Helsinki')

def fetch_recent_tracks():
    """Fetches the most recent tracks played by the user and saves them to a CSV file."""
    LIMIT = 50

    # Calculate the start of the current day
    now = datetime.now(tz=local_timezone)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_day_utc = start_of_day.astimezone(pytz.utc)

    # Fetch all tracks played from the start of the day
    try:
        results = sp.current_user_recently_played(limit=LIMIT, after=int(start_of_day_utc.timestamp() * 1000))
    except spotipy.exceptions.SpotifyException as e:
        print(f"Spotify API error: {e}")
        return

    track_data = []
    while results:
        tracks = results.get('items', [])
        if not tracks:
            break
        
        for item in tracks:
            track = item['track']
            primary_artist_id = track['artists'][0]['id']
            artist_info = sp.artist(primary_artist_id)
            genres = artist_info['genres']

            played_at_utc = pd.to_datetime(item['played_at'])
            played_at_local = played_at_utc.astimezone(local_timezone)
            played_at_formatted = played_at_local.strftime('%Y-%m-%d %H:%M:%S')

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
                'track_duration_ms': track['duration_ms'] / 1000,
                'popularity': track['popularity'],
                'artist_genres': ', '.join(genres),

                'album_name': track['album']['name'],
                'album_release_date': track['album']['release_date'],
                'album_id': track['album']['id'],
                'track_id': track['id'],
                'artist_ids': ', '.join([artist['id'] for artist in track['artists']]),

                'playlist_name': playlist_name,  # Add playlist name (if available)
                'playlist_id': playlist_id,  # Add playlist ID (if available)
            })

        if results['next']:
            results = sp.next(results)
        else:
            break

    df = pd.DataFrame(track_data)
    
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)

    filename = os.path.join(DATA_PATH, "recent_tracks.csv")

    # Append data to the existing CSV file, or create it if it doesn't exist
    if os.path.exists(filename):
        existing_data = pd.read_csv(filename)
        combined_data = pd.concat([existing_data, df])
        # Remove duplicates based on 'played_at' column
        combined_data.drop_duplicates(subset=['played_at'], keep='first', inplace=True)
        # Sort the data by 'played_at' in ascending order (oldest first, newest last)
        combined_data['played_at'] = pd.to_datetime(combined_data['played_at'])
        combined_data = combined_data.sort_values(by='played_at', ascending=True)
        combined_data.to_csv(filename, mode='w', header=True, index=False)
    else:
        # If the file doesn't exist, create it and save the data
        df.to_csv(filename, mode='w', header=True, index=False)

    print(f"Recent tracks saved to {filename}")

if __name__ == "__main__":
    fetch_recent_tracks()
