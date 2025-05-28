
import joblib
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
import os 
import boto3
import io

timelabel = datetime.today() - relativedelta(days=1)
timelabel = timelabel.strftime('%Y_%m_%d')

bucket_name = "soccer-storage"
feature_scaler_source = "webapp-storage/encoder/feature_scaler.pkl"
target_scaler_source = "webapp-storage/encoder/target_scaler.pkl"
label_encoder_source = "webapp-storage/encoder/label_encoder.pkl"
# model_source = "s3://soccer-storage/webapp-storage/model/lstm_model_3.h5"
# feature_scaler_source = 'src/baq/libs/utils/feature_scaler.pkl'
# target_scaler_source = 'src/baq/libs/utils/target_scaler.pkl'
# label_encoder_source = 'src/baq/libs/utils/label_encoder.pkl'
model_source = 'src/baq/libs/models/lstm_model_3.h5'
raw_data_source = f's3://soccer-storage/webapp-storage/data/raw/raw_data_{timelabel}.csv'
processed_data_source = f's3://soccer-storage/webapp-storage/data/processed/processed_data_{timelabel}.csv'  #save path
seasonal_medians_source = "s3://soccer-storage/webapp-storage/data/raw/seasonal_medians.csv"

def save_data(data, path=processed_data_source):
    # label = datetime.today() - relativedelta(days=1)
    # label = label.strftime('%Y_%m_%d')
    
    # s3_path = f's3://soccer-storage/webapp-storage/data/processed/processed_data_{label}.csv'
    
    data.to_csv(path, index=False)

def load_pklmodel(key: str, bucket: str = "soccer-storage"):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    with io.BytesIO(response['Body'].read()) as f:
        model = joblib.load(f)
    return model


def pm25_to_aqi_tier(pm25):
    if pm25 <= 12.0:
        return 0  # Good
    elif pm25 <= 35.4:
        return 1  # Moderate
    elif pm25 <= 55.4:
        return 2  # Unhealthy SG
    elif pm25 <= 150.4:
        return 3  # Unhealthy
    elif pm25 <= 250.4:
        return 4  # Very Unhealthy
    else:
        return 5  # Hazardous

def fill_missing_with_seasonal_median(df, seasonal_medians_csv):
    df['hour'] = df.index.hour
    df['day'] = df.index.day
    df['month'] = df.index.month

    stats_df = pd.read_csv(seasonal_medians_csv)

    for _, row in stats_df.iterrows():
        col = row['feature']
        condition = (
            (df['month'] == row['month']) &
            (df['day'] == row['day']) &
            (df['hour'] == row['hour']) &
            (df[col].isna())
        )
        df.loc[condition, col] = row['median']

    df.drop(columns=['hour', 'day', 'month'], inplace=True)
    return df

def clean_data(df):
    # Rename columns
    df.columns = (
        df.columns
        .str.replace('\s*\(\)', '', regex=True)
        .str.replace(' ', '_', regex=False)
    )

    # Set index
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df.set_index('time', inplace=True)
    df.sort_index(inplace=True)

    # Drop columns
    df.drop(columns=['carbon_dioxide_(ppm)', 'methane_(μg/m³)', 'snowfall_(cm)', 'snow_depth_(m)'], inplace=True)

    # Perform linear interpolation
    df = df.resample('1h').asfreq()
    df = df.interpolate(method='linear')

    # Fill missing values
    df = fill_missing_with_seasonal_median(df, seasonal_medians_source)

    # Encode weather code
    le = load_pklmodel(label_encoder_source, bucket_name)
    df['weather_code_(wmo_code)'] = df['weather_code_(wmo_code)'].astype(str)
    df['weather_code_(wmo_code)'] = le.transform(df['weather_code_(wmo_code)'])

    return df

def feature_engineering(df):
    # Time-based features
    df['hour'] = df.index.hour
    df['dayofweek'] = df.index.dayofweek
    df['month'] = df.index.month
    df['is_weekend'] = df['dayofweek'].isin([5, 6]).astype(int)
    df['is_night'] = df['hour'].apply(lambda h: 1 if (h < 6 or h >= 20) else 0)
    df['sin_hour'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['cos_hour'] = np.cos(2 * np.pi * df['hour'] / 24)

    # Lag features
    lags = [1, 3, 6, 12, 24]
    for lag in lags:
        for col in ['pm2_5_(μg/m³)', 'pm10_(μg/m³)', 'ozone_(μg/m³)', 'dust_(μg/m³)']:
            df[f'{col}_lag{lag}'] = df[col].shift(lag)

    # Rolling features
    windows = [3, 6, 12]
    for window in windows:
        for col in ['pm2_5_(μg/m³)', 'pm10_(μg/m³)', 'ozone_(μg/m³)']:
            df[f'{col}_rollmean{window}'] = df[col].rolling(window=window).mean()

    # Air quality index tier
    df['pm2_5_tier'] = df['pm2_5_(μg/m³)'].apply(pm25_to_aqi_tier)
    df.dropna(inplace=True)

    return df

def normalize_data(df, feature_cols, target_col):
    feature_scaler = load_pklmodel(feature_scaler_source, bucket_name)
    target_scaler = load_pklmodel(target_scaler_source, bucket_name)

    df.loc[:, feature_cols] = feature_scaler.transform(df[feature_cols])
    df.loc[:, [target_col]] = target_scaler.transform(df[[target_col]])

    return df

def create_sequences(data, target_column, sequence_length):
    X = []
    feature_data = data.drop(target_column, axis=1).values

    for i in range(len(data) - sequence_length + 1):
        X.append(feature_data[i:i + sequence_length])

    return np.array(X)

def preprocess_data(datapath = raw_data_source):
    df = pd.read_csv(datapath)
    df = df.tail(48)
    df = clean_data(df)
    df = feature_engineering(df)

    feature_cols = [col for col in df.columns if col not in [
        'pm2_5_(μg/m³)', 'hour', 'dayofweek', 'month', 'is_weekend', 'is_night', 'sin_hour', 'cos_hour', 'weather_code_(wmo_code)', 'pm2_5_tier'
    ]]
    target_col = 'pm2_5_(μg/m³)'

    df = normalize_data(df, feature_cols, target_col)
    # X = create_sequences(df, target_col, sequence_length)
    # np.save('data/x.npy', X)
    df.to_csv(processed_data_source)
    return df


if __name__ == "__main__":
    preprocess_data(raw_data_source)
    print("Data preprocessing completed and saved to S3.")


    ## Uncomment the following lines to run 1-time prediction
    # df = pd.read_csv(processed_data_source)
    # lstm_model = load_model(model_source, compile=False)
    # print(one_time_prediction(lstm_model, df, target_col='pm2_5', sequence_length=24))


    # Uncomment the following lines to run rolling forecast
    # df = pd.read_csv(processed_data_source)
    # lstm_model = load_model(model_source, compile=False)
    # print(rolling_forecast(lstm_model, df, target_col='pm2_5', sequence_length=24, forecast_horizon=48))

