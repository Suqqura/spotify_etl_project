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

# how many tracks/artists to fetch
LIMIT = 10

# artists
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

# tracks
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
    fetch_top_artists()
    fetch_top_tracks()