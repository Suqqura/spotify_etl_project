import pandas as pd
import os

def transform_spotify_data():
    filename = 'data/recent_tracks.csv'
    
    if not os.path.exists(filename):
        print(f"Tiedostoa {filename} ei löydy. Varmista, että olet ajanut fetch_spotify_data.py.")
        return
    
    df = pd.read_csv(filename)
    
    # Analyze data
    top_tracks = df.groupby('track_name').size().reset_index(name='plays').sort_values(by='plays', ascending=False)
    top_artists = df.groupby('artist_name').size().reset_index(name='plays').sort_values(by='plays', ascending=False)

    # Save data
    top_tracks.to_csv('data/top_tracks.csv', mode='w', header=True, index=False)
    top_artists.to_csv('data/top_artists.csv', mode='w', header=True, index=False)

    print("Transformed data saved to data/top_tracks.csv and data/top_artists.csv")

if __name__ == "__main__":
    transform_spotify_data()
