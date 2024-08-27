import os
import pandas as pd

DATA_PATH = '/home/hh/airflow/dags/spotify_project/data/'

def transform_recent_tracks():
    """Transforms the raw recent tracks data and saves it to a new CSV file."""
    raw_filename = os.path.join(DATA_PATH, "recent_tracks_raw.csv")
    transformed_filename = os.path.join(DATA_PATH, "recent_tracks.csv")

    if not os.path.exists(raw_filename):
        print(f"No raw data found at {raw_filename}")
        return

    # Load the raw data
    df = pd.read_csv(raw_filename)

    # Remove duplicates based on 'played_at' column
    df.drop_duplicates(subset=['played_at'], keep='first', inplace=True)

    # Sort the data by 'played_at' in ascending order (oldest first, newest last)
    df['played_at'] = pd.to_datetime(df['played_at'])
    df = df.sort_values(by='played_at', ascending=True)
    
    # Convert track_duration_ms to seconds for calculation
    df['track_duration_seconds'] = df['track_duration_ms'] // 1000
    
    # Convert track_duration_ms to mm:ss format
    df['track_duration'] = df['track_duration_ms'].apply(lambda x: "{:d}:{:02d}".format(int(x // 60000), int((x % 60000) // 1000)))

    # Drop the original 'track_duration_ms' column
    df.drop(columns=['track_duration_ms'], inplace=True)

    # Save the transformed data to a new CSV file
    df.to_csv(transformed_filename, mode='w', header=True, index=False)

    print(f"Transformed tracks saved to {transformed_filename}")

if __name__ == "__main__":
    transform_recent_tracks()
