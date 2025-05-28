from datetime import datetime
from dateutil.relativedelta import relativedelta
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

timelabel = datetime.today() - relativedelta(days=1)
timelabel = timelabel.strftime('%Y_%m_%d')

# Define the S3 path
raw_data_path = f's3://soccer-storage/webapp-storage/data/raw/raw_data_{timelabel}.csv'


# this function query and preprocess data  
def get_data():
    weather_data = get_weather_data()
    air_quality = get_air_quality()
    weather_data = clean_data(weather_data)
    air_quality = clean_data(air_quality)

    combined_data = pd.concat([weather_data, air_quality], axis=1)

    # Filter out rows with missing temperature
    combined_data = combined_data[combined_data['temperature_2m'].notnull()]
    rename_map ={
    'temperature_2m': 'temperature_2m (°C)',
    'relative_humidity_2m': 'relative_humidity_2m (%)',
    'dew_point_2m': 'dew_point_2m (°C)',
    'apparent_temperature': 'apparent_temperature (°C)',
    'precipitation': 'precipitation (mm)',
    'rain': 'rain (mm)',
    'snowfall': 'snowfall (cm)',
    'snow_depth': 'snow_depth (m)',
    'weather_code': 'weather_code (wmo code)',
    'pressure_msl': 'pressure_msl (hPa)',
    'surface_pressure': 'surface_pressure (hPa)',
    'cloud_cover': 'cloud_cover (%)',
    'cloud_cover_low': 'cloud_cover_low (%)',
    'cloud_cover_mid': 'cloud_cover_mid (%)',
    'cloud_cover_high': 'cloud_cover_high (%)',
    'et0_fao_evapotranspiration': 'et0_fao_evapotranspiration (mm)',
    'vapour_pressure_deficit': 'vapour_pressure_deficit (kPa)',
    'wind_speed_10m': 'wind_speed_10m (km/h)',
    'wind_speed_100m': 'wind_speed_100m (km/h)',
    'wind_direction_10m': 'wind_direction_10m (°)',
    'wind_direction_100m': 'wind_direction_100m (°)',
    'wind_gusts_10m': 'wind_gusts_10m (km/h)',
    'soil_temperature_0_to_7cm': 'soil_temperature_0_to_7cm (°C)',
    'soil_temperature_7_to_28cm': 'soil_temperature_7_to_28cm (°C)',
    'soil_temperature_28_to_100cm': 'soil_temperature_28_to_100cm (°C)',
    'soil_temperature_100_to_255cm': 'soil_temperature_100_to_255cm (°C)',
    'soil_moisture_0_to_7cm': 'soil_moisture_0_to_7cm (m³/m³)',
    'soil_moisture_7_to_28cm': 'soil_moisture_7_to_28cm (m³/m³)',
    'soil_moisture_28_to_100cm': 'soil_moisture_28_to_100cm (m³/m³)',
    'soil_moisture_100_to_255cm': 'soil_moisture_100_to_255cm (m³/m³)',
    'pm10': 'pm10 (μg/m³)',
    'pm2_5': 'pm2_5 (μg/m³)',
    'carbon_monoxide': 'carbon_monoxide (μg/m³)',
    'carbon_dioxide': 'carbon_dioxide (ppm)',
    'nitrogen_dioxide': 'nitrogen_dioxide (μg/m³)',
    'sulphur_dioxide': 'sulphur_dioxide (μg/m³)',
    'ozone': 'ozone (μg/m³)',
    'methane': 'methane (μg/m³)',
    'uv_index_clear_sky': 'uv_index_clear_sky ()',
    'uv_index': 'uv_index ()',
    'dust': 'dust (μg/m³)',
    'aerosol_optical_depth': 'aerosol_optical_depth ()'
    }
    df = pd.DataFrame()
    for old_name, new_name in rename_map.items():
        df[new_name] = combined_data[old_name]

    save_data(df)
    
def get_weather_data(start_date = (datetime.today() - relativedelta(years=2)).strftime('%Y-%m-%d'),
                     end_date = (datetime.today()).strftime('%Y-%m-%d'), # - relativedelta(hours=47)
										 lat = 13.7563,
										 lon = 100.5018):
	# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	url = "https://archive-api.open-meteo.com/v1/archive"
	params = {
		"latitude": lat,
		"longitude": lon,
		"start_date": start_date,
		"end_date": end_date,
		"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation", "rain", "snowfall", "snow_depth", "weather_code", "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_100m", "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m", "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm", "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm", "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm", "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm"],

	}
	responses = openmeteo.weather_api(url, params=params)

	# Process first location. Add a for-loop for multiple locations or weather models
	response = responses[0]
	# print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
	# print(f"Elevation {response.Elevation()} m asl")
	# print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
	# print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

	# Process hourly data. The order of variables needs to be the same as requested.
	hourly = response.Hourly()
	hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
	hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
	hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
	hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
	hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
	hourly_rain = hourly.Variables(5).ValuesAsNumpy()
	hourly_snowfall = hourly.Variables(6).ValuesAsNumpy()
	hourly_snow_depth = hourly.Variables(7).ValuesAsNumpy()
	hourly_weather_code = hourly.Variables(8).ValuesAsNumpy()
	hourly_pressure_msl = hourly.Variables(9).ValuesAsNumpy()
	hourly_surface_pressure = hourly.Variables(10).ValuesAsNumpy()
	hourly_cloud_cover = hourly.Variables(11).ValuesAsNumpy()
	hourly_cloud_cover_low = hourly.Variables(12).ValuesAsNumpy()
	hourly_cloud_cover_mid = hourly.Variables(13).ValuesAsNumpy()
	hourly_cloud_cover_high = hourly.Variables(14).ValuesAsNumpy()
	hourly_et0_fao_evapotranspiration = hourly.Variables(15).ValuesAsNumpy()
	hourly_vapour_pressure_deficit = hourly.Variables(16).ValuesAsNumpy()
	hourly_wind_speed_10m = hourly.Variables(17).ValuesAsNumpy()
	hourly_wind_speed_100m = hourly.Variables(18).ValuesAsNumpy()
	hourly_wind_direction_10m = hourly.Variables(19).ValuesAsNumpy()
	hourly_wind_direction_100m = hourly.Variables(20).ValuesAsNumpy()
	hourly_wind_gusts_10m = hourly.Variables(21).ValuesAsNumpy()
	hourly_soil_temperature_0_to_7cm = hourly.Variables(22).ValuesAsNumpy()
	hourly_soil_temperature_7_to_28cm = hourly.Variables(23).ValuesAsNumpy()
	hourly_soil_temperature_28_to_100cm = hourly.Variables(24).ValuesAsNumpy()
	hourly_soil_temperature_100_to_255cm = hourly.Variables(25).ValuesAsNumpy()
	hourly_soil_moisture_0_to_7cm = hourly.Variables(26).ValuesAsNumpy()
	hourly_soil_moisture_7_to_28cm = hourly.Variables(27).ValuesAsNumpy()
	hourly_soil_moisture_28_to_100cm = hourly.Variables(28).ValuesAsNumpy()
	hourly_soil_moisture_100_to_255cm = hourly.Variables(29).ValuesAsNumpy()

	hourly_data = {"date": pd.date_range(
		start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
		end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
		freq = pd.Timedelta(seconds = hourly.Interval()),
		inclusive = "left"
	)}

	hourly_data["temperature_2m"] = hourly_temperature_2m
	hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
	hourly_data["dew_point_2m"] = hourly_dew_point_2m
	hourly_data["apparent_temperature"] = hourly_apparent_temperature
	hourly_data["precipitation"] = hourly_precipitation
	hourly_data["rain"] = hourly_rain
	hourly_data["snowfall"] = hourly_snowfall
	hourly_data["snow_depth"] = hourly_snow_depth
	hourly_data["weather_code"] = hourly_weather_code
	hourly_data["pressure_msl"] = hourly_pressure_msl
	hourly_data["surface_pressure"] = hourly_surface_pressure
	hourly_data["cloud_cover"] = hourly_cloud_cover
	hourly_data["cloud_cover_low"] = hourly_cloud_cover_low
	hourly_data["cloud_cover_mid"] = hourly_cloud_cover_mid
	hourly_data["cloud_cover_high"] = hourly_cloud_cover_high
	hourly_data["et0_fao_evapotranspiration"] = hourly_et0_fao_evapotranspiration
	hourly_data["vapour_pressure_deficit"] = hourly_vapour_pressure_deficit
	hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
	hourly_data["wind_speed_100m"] = hourly_wind_speed_100m
	hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
	hourly_data["wind_direction_100m"] = hourly_wind_direction_100m
	hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
	hourly_data["soil_temperature_0_to_7cm"] = hourly_soil_temperature_0_to_7cm
	hourly_data["soil_temperature_7_to_28cm"] = hourly_soil_temperature_7_to_28cm
	hourly_data["soil_temperature_28_to_100cm"] = hourly_soil_temperature_28_to_100cm
	hourly_data["soil_temperature_100_to_255cm"] = hourly_soil_temperature_100_to_255cm
	hourly_data["soil_moisture_0_to_7cm"] = hourly_soil_moisture_0_to_7cm
	hourly_data["soil_moisture_7_to_28cm"] = hourly_soil_moisture_7_to_28cm
	hourly_data["soil_moisture_28_to_100cm"] = hourly_soil_moisture_28_to_100cm
	hourly_data["soil_moisture_100_to_255cm"] = hourly_soil_moisture_100_to_255cm

	weather_data = pd.DataFrame(data = hourly_data)
	weather_data  = weather_data[weather_data["temperature_2m"].notnull()]
	return weather_data

def get_air_quality(start_date = (datetime.today() - relativedelta(years=2)).strftime('%Y-%m-%d'),
                    end_date = (datetime.today()).strftime('%Y-%m-%d'), # - relativedelta(days=1)
										lat = 13.7563,
										lon = 100.5018):
  # Setup the Open-Meteo API client with cache and retry on error
  cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
  retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
  openmeteo = openmeteo_requests.Client(session = retry_session)

  # Make sure all required weather variables are listed here
  # The order of variables in hourly or daily is important to assign them correctly below
  url = "https://air-quality-api.open-meteo.com/v1/air-quality"
  params = {
    "latitude": lat,
    "longitude": lon,
    "hourly": ["pm10", "pm2_5", "carbon_monoxide", "carbon_dioxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "aerosol_optical_depth", "uv_index", "dust", "uv_index_clear_sky", "ammonia", "methane"],
    "start_date": start_date,
    "end_date": end_date,
    #"timezone": "Asia/Bangkok"
  }
  responses = openmeteo.weather_api(url, params=params)

  # Process first location. Add a for-loop for multiple locations or weather models
  response = responses[0]
  # print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
  # print(f"Elevation {response.Elevation()} m asl")
  # print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
  # print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

  # Process hourly data. The order of variables needs to be the same as requested.
  hourly = response.Hourly()
  hourly_pm10 = hourly.Variables(0).ValuesAsNumpy()
  hourly_pm2_5 = hourly.Variables(1).ValuesAsNumpy()
  hourly_carbon_monoxide = hourly.Variables(2).ValuesAsNumpy()
  hourly_carbon_dioxide = hourly.Variables(3).ValuesAsNumpy()
  hourly_nitrogen_dioxide = hourly.Variables(4).ValuesAsNumpy()
  hourly_sulphur_dioxide = hourly.Variables(5).ValuesAsNumpy()
  hourly_ozone = hourly.Variables(6).ValuesAsNumpy()
  hourly_aerosol_optical_depth = hourly.Variables(7).ValuesAsNumpy()
  hourly_uv_index = hourly.Variables(8).ValuesAsNumpy()
  hourly_dust = hourly.Variables(9).ValuesAsNumpy()
  hourly_uv_index_clear_sky = hourly.Variables(10).ValuesAsNumpy()
  #hourly_ammonia = hourly.Variables(11).ValuesAsNumpy()
  hourly_methane = hourly.Variables(12).ValuesAsNumpy()

  hourly_data = {"date": pd.date_range(
    start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
    end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
    freq = pd.Timedelta(seconds = hourly.Interval()),
    inclusive = "left"
  )}

  hourly_data["pm10"] = hourly_pm10
  hourly_data["pm2_5"] = hourly_pm2_5
  hourly_data["carbon_monoxide"] = hourly_carbon_monoxide
  hourly_data["carbon_dioxide"] = hourly_carbon_dioxide
  hourly_data["nitrogen_dioxide"] = hourly_nitrogen_dioxide
  hourly_data["sulphur_dioxide"] = hourly_sulphur_dioxide
  hourly_data["ozone"] = hourly_ozone
  hourly_data["aerosol_optical_depth"] = hourly_aerosol_optical_depth
  hourly_data["uv_index"] = hourly_uv_index
  hourly_data["dust"] = hourly_dust
  hourly_data["uv_index_clear_sky"] = hourly_uv_index_clear_sky
  #hourly_data["ammonia"] = hourly_ammonia
  hourly_data["methane"] = hourly_methane

  air_quality = pd.DataFrame(data = hourly_data)
  return air_quality

def clean_data(df):
    df['time'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S', errors='coerce', utc=False)
    df['time'] = df['time'].dt.tz_convert('Asia/Bangkok')
    df['time'] = df['time'].dt.tz_localize(None)
    df.drop(columns = ["date"], inplace = True)
    df.set_index('time', inplace=True)
    df.sort_index(inplace=True)
    #df.dropna(how='all', inplace=True)
    return df

def save_data(data, data_path =raw_data_path):
    
    # Save to S3 (requires s3fs installed and AWS credentials configured)
    data.to_csv(data_path)


if __name__ == "__main__":
    get_data()