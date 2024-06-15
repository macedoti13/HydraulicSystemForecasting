from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from xgboost import XGBRegressor
import pandas as pd
import pickle

ORIGIN_DATA_PATH = "../data/training/water_consumption_training.parquet"
MODEL_PATH = "../models/xgb_flow_out_forecast_3.pkl"

def set_training_data(df):
    weather_columns = [
    'total_precip_mm', 
    'station_pressure_mb', 
    'max_pressure_last_hour_mb',
    'min_pressure_last_hour_mb', 
    'global_radiation_kj_m2', 
    'air_temp_c',
    'dew_point_temp_c', 
    'max_temp_last_hour_c', 
    'min_temp_last_hour_c',
    'max_dew_point_last_hour_c', 
    'min_dew_point_last_hour_c',
    'max_humidity_last_hour_percentage', 
    'min_humidity_last_hour_percentage',
    'relative_humidity_percentage', 
    'wind_direction_deg', 
    'max_wind_gust_m_s',
    'wind_speed_m_s'
    ]

    weather_feature_columns = []

    for window_name in ['24_hours', '10_hours', '1_hour', '10_minutes']:
        weather_feature_columns.extend([f'average_{col}_last_{window_name}' for col in weather_columns])
        weather_feature_columns.extend([f'last_{col}' for col in weather_columns])

    # Include timestamp in the list
    columns_to_exclude = ['timestamp'] + weather_feature_columns
    training_df = df.drop(columns=columns_to_exclude)
    
    return training_df

def train_test_split(original_df):
    training_df = original_df.copy()
    train = training_df[training_df['year'] == 2023]
    test = training_df[training_df['year'] == 2024]

    # List of features for training (excluding 'output_flow_rate')
    training_features = [col for col in training_df.columns if col != 'output_flow_rate']

    # Create X and y
    X_train = train[training_features]
    y_train = train['output_flow_rate']
    X_test = test[training_features]
    y_test = test['output_flow_rate']
    
    return X_train, y_train, X_test, y_test

def set_model_training_pipeline() -> GridSearchCV:
    # Initialize the XGBoost regressor with specified hyperparameters
    model = XGBRegressor(
        n_estimators=100000, 
        learning_rate=0.01, 
        early_stopping_rounds=100
    )
    
    # Define time series cross-validation strategy
    cv = TimeSeriesSplit(n_splits=5)
    
    # Define an expanded parameter grid for hyperparameter tuning
    params = {
        'n_estimators': [100, 500, 1000, 5000],
        'max_depth': [3, 5, 10, 14],
        'learning_rate': [0.01, 0.05, 0.1]
    }
    
    # Initialize GridSearchCV with the model, parameter grid, and cross-validation strategy
    clf = GridSearchCV(
        estimator=model, 
        param_grid=params, 
        cv=cv, 
        scoring='neg_mean_squared_error',
        n_jobs=-1,  # Use all available cores
        verbose=2
    )
    
    return clf

def train_model(clf: GridSearchCV, x_train: pd.DataFrame, y_train: pd.Series, x_test: pd.DataFrame, y_test: pd.Series) -> GridSearchCV:
    # Train the model with training and validation data
    clf.fit(
        x_train, y_train,
        eval_set=[(x_train, y_train), (x_test, y_test)],
        verbose=100
    )
    
    return clf.best_estimator_

def main():
    df = pd.read_parquet(ORIGIN_DATA_PATH)
    training_df = set_training_data(df)
    X_train, y_train, X_test, y_test = train_test_split(training_df)
    clf = set_model_training_pipeline()
    clf = train_model(clf, X_train, y_train, X_test, y_test)
    pickle.dump(clf, open(MODEL_PATH, 'wb'))
    
if __name__ == "__main__":
    main()
