from pathlib import Path
import pandas as pd

CURATED_DATA_PATH = Path('../data/curated/water_consumption_curated.parquet')
OUTPUT_SAVE_PATH = Path('../data/questions/q3_avg_pumps_use_peak_&_offpeak_hours.parquet')

def get_avg_use_per_bomb_in_minutes_corrected(original_df: pd.DataFrame) -> pd.DataFrame:
    df = original_df.copy()
    
    # Ensure timestamp is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Create date column
    df['date'] = df['timestamp'].dt.date
    
    # Calculate peak hours
    peak_hours = (df["hour"] >= 18) & (df["hour"] <= 21)
    df["is_peak_hour"] = peak_hours
    
    # Calculate duration of pumps
    df['pump_1_duration'] = df['pump_1'] * df['time_passed_seconds']
    df['pump_2_duration'] = df['pump_2'] * df['time_passed_seconds']
    
    # Sum water bombs usage time per day and hour
    daily_peak_usage = df[df['is_peak_hour']].groupby('date').agg({'pump_1_duration': 'sum', 'pump_2_duration': 'sum'})
    daily_off_peak_usage = df[~df['is_peak_hour']].groupby('date').agg({'pump_1_duration': 'sum', 'pump_2_duration': 'sum'})

    # Calculate water bombs average usage time per day in minutes
    gmb_1_peak_avg = daily_peak_usage['pump_1_duration'].mean() / 60  # convert seconds to minutes
    gmb_1_off_peak_avg = daily_off_peak_usage['pump_1_duration'].mean() / 60  # convert seconds to minutes

    gmb_2_peak_avg = daily_peak_usage['pump_2_duration'].mean() / 60  # convert seconds to minutes
    gmb_2_off_peak_avg = daily_off_peak_usage['pump_2_duration'].mean() / 60  # convert seconds to minutes
    
    def convert_to_hours_and_minutes(minutes):
        if pd.isna(minutes):
            return "0 hours and 0 minutes"
        total_minutes = int(minutes)
        hours = total_minutes // 60
        minutes = total_minutes % 60
        return f"{hours} hours and {minutes} minutes"
    
    data = {
        'pump': ['pump_1', 'pump_2'],
        'average_time_used_peak_hours': [
            convert_to_hours_and_minutes(gmb_1_peak_avg), 
            convert_to_hours_and_minutes(gmb_2_peak_avg)
        ],
        'average_time_used_offpeak_hours': [
            convert_to_hours_and_minutes(gmb_1_off_peak_avg), 
            convert_to_hours_and_minutes(gmb_2_off_peak_avg)
        ]
    }
    
    result_df = pd.DataFrame(data)
    
    return result_df

def main():
    df = pd.read_parquet(CURATED_DATA_PATH)
    result_df = get_avg_use_per_bomb_in_minutes_corrected(df)
    result_df.to_parquet(OUTPUT_SAVE_PATH, index=False)
    
if __name__ == '__main__':
    main()