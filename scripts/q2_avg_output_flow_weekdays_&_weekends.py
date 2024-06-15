from pathlib import Path
import pandas as pd

CURATED_DATA_PATH = Path("../data/curated/water_consumption_curated.parquet")
OUTPUT_SAVE_PATH = Path("../data/questions/q2_avg_output_flow_weekdays_&_weekends.parquet")

def get_average_flow_out_across_day(original_df: pd.DataFrame) -> pd.DataFrame:
    df = original_df.copy()
    
    # Separate data into weekdays and weekends
    df_weekdays = df[df['weekday'] < 5]
    df_weekends = df[df['weekday'] >= 5]

    # Calculate average flow out per hour for weekdays and weekends
    flow_out_weekdays = df_weekdays.groupby('hour')['output_flow_rate'].mean().reset_index().rename(columns={'output_flow_rate': 'avg_weekday_output_flow'})
    flow_out_weekends = df_weekends.groupby('hour')['output_flow_rate'].mean().reset_index().rename(columns={'output_flow_rate': 'avg_weekend_output_flow'})
    
    # Merge the weekday and weekend data into a single DataFrame
    df_combined = pd.merge(flow_out_weekdays, flow_out_weekends, on='hour', how='outer')
    
    return df_combined

def main():
    df = pd.read_parquet(CURATED_DATA_PATH)
    df_avg_output_flow = get_average_flow_out_across_day(df)
    df_avg_output_flow.to_parquet(OUTPUT_SAVE_PATH)
    
if __name__ == "__main__":
    main()