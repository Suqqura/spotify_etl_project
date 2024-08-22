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

# Create Spotipy client
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope="user-read-recently-played user-top-read"
))

# How many tracks/artists to fetch
LIMIT = 10

# Set your local timezone
local_timezone = pytz.timezone('Europe/Helsinki')

# Fetch the user's most recent tracks
def fetch_recent_tracks():
    """Fetches the most recent tracks played by the user and saves them to a CSV file."""
    results = sp.current_user_recently_played(limit=LIMIT)
    tracks = results.get('items', [])

    if not tracks:
        print("No recent tracks found.")
        return

    # Convert data to DataFrame
    track_data = []
    for item in tracks:
        track = item['track']
        primary_artist_id = track['artists'][0]['id']

        # Fetch genres for the primary artist
        artist_info = sp.artist(primary_artist_id)
        genres = artist_info['genres']

        # Convert the UTC time to local time and format it
        played_at_utc = pd.to_datetime(item['played_at'])
        played_at_local = played_at_utc.astimezone(local_timezone)
        played_at_formatted = played_at_local.strftime('%Y-%m-%d %H:%M:%S')

        track_data.append({
            'played_at': played_at_formatted,
            'track_name': track['name'],
            'artist_name': ', '.join([artist['name'] for artist in track['artists']]),
            'album_name': track['album']['name'],
            'genre': ', '.join(genres),
            'popularity': track['popularity'],
            'track_id': track['id'],
        })

    df = pd.DataFrame(track_data)
    
    # Create data directory if it doesn't exist
    if not os.path.exists('data'):
        os.makedirs('data')

    # Save data to CSV file
    timestamp = datetime.now().strftime('%d%m%Y_%H%M%S') 
    filename = f'data/recent_tracks_{timestamp}.csv'
    df.to_csv(filename, index=False)

    print(f"Recent tracks saved to {filename}")
    
def fetch_top_artists():
    """Fetches and prints the user's top artists across different time ranges."""
    ranges = ['short_term', 'medium_term', 'long_term']

    for sp_range in ranges:
        print(f"Top artists for time range: {sp_range}")

        # Fetch top artists for the specified time range
        results = sp.current_user_top_artists(time_range=sp_range, limit=LIMIT)

        # Print the rank and name of each artist
        for i, item in enumerate(results['items']):
            print(f"{i + 1}: {item['name']}")
        print()  # Blank line for better readability between ranges

def fetch_top_tracks():
    """Fetches and prints the user's top tracks."""
    print("Top tracks:")

    # Fetch top tracks for the long-term time range
    results = sp.current_user_top_tracks(time_range='long_term', limit=LIMIT)

    # Print the rank, track name, and artist name
    for i, item in enumerate(results['items']):
        track_name = item['name']
        artist_name = ', '.join([artist['name'] for artist in item['artists']])
        print(f"{i + 1}: {track_name} by {artist_name}")
    print()  # Blank line for better readability

if __name__ == "__main__":
    fetch_recent_tracks()  # Fetch and save the most recent tracks
    fetch_top_artists()    # Print the user's top artists for different time ranges
    fetch_top_tracks()     # Print the user's top tracks
