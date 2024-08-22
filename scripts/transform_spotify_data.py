import pandas as pd

def transform_spotify_data():
    df = pd.read_csv('data/recent_tracks.csv')
    
    # analyze data
    top_tracks = df.groupby('track_name').size().reset_index(name='plays').sort_values(by='plays', ascending=False)
    top_artists = df.groupby('artist_name').size().reset_index(name='plays').sort_values(by='plays', ascending=False)

    # save data
    top_tracks.to_csv('data/top_tracks.csv', mode='w', header=True, index=False)
    top_artists.to_csv('data/top_artists.csv', mode='w', header=True, index=False)

    print("Transformed data saved to data/top_tracks.csv and data/top_artists.csv")

if __name__ == "__main__":
    transform_spotify_data()
