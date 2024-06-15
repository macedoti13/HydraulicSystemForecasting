from pathlib import Path
import pandas as pd
import numpy as np

def impute_faulty_data(df):
    """
    Impute reservoir_level_percentage and input_flow_rate for rows with reservoir_level_percentage set to 0.0.

    Parameters
    ----------
    df : pd.DataFrame
        The original DataFrame with the raw data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with imputed reservoir_level_percentage and input_flow_rate values.
    """
    df = df.copy()
    faulty_rows = df[df['reservoir_level_percentage'] == 0.0]

    for index, row in faulty_rows.iterrows():
        t_minus_1 = df.loc[:index-1, 'reservoir_level_percentage'].replace(0.0, pd.NA).last_valid_index()
        t_plus_1 = df.loc[index+1:, 'reservoir_level_percentage'].replace(0.0, pd.NA).first_valid_index()
        
        if pd.notna(t_minus_1) and pd.notna(t_plus_1):
            prev_level = df.at[t_minus_1, 'reservoir_level_percentage']
            next_level = df.at[t_plus_1, 'reservoir_level_percentage']
            if prev_level < next_level:
                imputed_level = round((prev_level + next_level) / 2, 2)
            else:
                imputed_level = round((prev_level + next_level) / 2, 2)

            df.at[index, 'reservoir_level_percentage'] = imputed_level
            
            prev_flow = df.at[t_minus_1, 'input_flow_rate']
            next_flow = df.at[t_plus_1, 'input_flow_rate']
            imputed_flow = round((prev_flow + next_flow) / 2, 2)
            df.at[index, 'input_flow_rate'] = imputed_flow

    return df

def fill_missing_pressure(df):
    """
    Fill missing pressure values using a moving average of the last 10 steps.

    Parameters
    ----------
    df : pd.DataFrame
        The original DataFrame with the raw data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with filled pressure values.
    """
    df = df.copy()
    df['pressure'] = df['pressure'].replace(0.0, np.nan)
    df['pressure'] = df['pressure'].fillna(df['pressure'].rolling(window=10, min_periods=1).mean())
    df['pressure'] = df['pressure'].fillna(df['pressure'].mean())
    df['pressure'] = df['pressure'].round(2)
    
    return df

def fix_pump_status(df):
    """
    Fix invalid pump status by turning on pump_2 where input_flow_rate is greater than 0 but both pumps are off.

    Parameters
    ----------
    df : pd.DataFrame
        The original DataFrame with the raw data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with corrected pump statuses.
    """
    df = df.copy()
    invalid_pump_rows = df[(df['pump_1'] == 0) & (df['pump_2'] == 0) & (df['input_flow_rate'] > 0)]

    for index, row in invalid_pump_rows.iterrows():
        df.at[index, 'pump_2'] = 1

    return df

def turn_off_pumps(df):
    """
    Turn off both pumps where input_flow_rate is 0 but at least one pump is on.

    Parameters
    ----------
    df : pd.DataFrame
        The original DataFrame with the raw data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with pumps turned off where necessary.
    """
    df = df.copy()
    invalid_rows = df[(df['input_flow_rate'] == 0) & ((df['pump_1'] == 1) | (df['pump_2'] == 1))]

    for index, row in invalid_rows.iterrows():
        df.at[index, 'pump_1'] = 0
        df.at[index, 'pump_2'] = 0

    return df

def create_necessary_columns(original_df):
    """
    Create additional necessary columns for analysis.

    Parameters
    ----------
    original_df : pd.DataFrame
        The original DataFrame with the raw data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with additional columns.
    """
    df = original_df.copy()
    df['reservoir_level_liters'] = df['reservoir_level_percentage'] * 1_000_000 / 100
    df['time_passed_seconds'] = df['timestamp'].diff().dt.total_seconds()
    df['total_liters_entered'] = df['input_flow_rate'] * df['time_passed_seconds']
    df['effective_liters_entered'] = df['reservoir_level_liters'].diff()
    df['total_liters_out'] = df['total_liters_entered'] - df['effective_liters_entered']
    df['output_flow_rate'] = df['total_liters_out'] / df['time_passed_seconds']
    df.loc[df['effective_liters_entered'] < 0, 'effective_liters_entered'] = 0
    df = df.drop(0)
    
    return df

def adjust_flow_rates(df):
    """
    Adjust input_flow_rate so that total_liters_entered is not less than effective_liters_entered.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame with calculated columns.

    Returns
    -------
    pd.DataFrame
        The DataFrame with adjusted input_flow_rate.
    """
    df = df.copy()
    problem_rows = df[df['total_liters_entered'] < df['effective_liters_entered']]
    
    for index, row in problem_rows.iterrows():
        df.at[index, 'input_flow_rate'] = df.at[index, 'effective_liters_entered'] / df.at[index, 'time_passed_seconds']
        df.at[index, 'total_liters_entered'] = df.at[index, 'effective_liters_entered']
        df.at[index, 'total_liters_out'] = 0
        df.at[index, 'output_flow_rate'] = 0
    
    return df

def fix_problematic_rows(df):
    """
    Apply all data cleaning functions to fix various issues in the dataset.

    Parameters
    ----------
    df : pd.DataFrame
        The original DataFrame with the raw data.

    Returns
    -------
    pd.DataFrame
        The cleaned DataFrame.
    """
    df = impute_faulty_data(df)
    df = fill_missing_pressure(df)
    df = fix_pump_status(df)
    df = turn_off_pumps(df)
    df = create_necessary_columns(df)
    df = adjust_flow_rates(df)
    df = df.round(2).reset_index().rename(columns={'index': 'id'})

    return df

def main():
    # Load dataset
    original_df_path = Path('../data/original_data/water_consumption.parquet')
    original_df = pd.read_parquet(original_df_path)

    # Rename columns and replace pump status values
    original_df = original_df.rename(columns={
        "DATA/HORA": "timestamp",
        "VAZÃO ENTRADA (L/S)": "input_flow_rate",
        "NÍVEL RESERVATÓRIO (%)": "reservoir_level_percentage",
        "PRESSÃO (mca)": "pressure",
        "GMB 1 (10 OFF/ 90 ON)": "pump_1",
        "GMB 2(10 OFF/ 90 ON)": "pump_2",
    }).replace({
        "pump_1": {10: 0, 90: 1},
        "pump_2": {10: 0, 90: 1}
    })

    # Fix problematic rows
    cleaned_df = fix_problematic_rows(original_df)

    # Save cleaned DataFrame to a new parquet file
    cleaned_df.to_parquet('../data/preprocessed_data/water_consumption_cleaned_0.parquet')

if __name__ == "__main__":
    main()
