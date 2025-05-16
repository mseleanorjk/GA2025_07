"""
Tool for handling OpenWeatherMap API requests and data files
"""

# Installation of libraries:
## !pip install openmeteo-requests
## !pip install requests-cache retry-requests numpy pandas

# Usage of the API, according to the documentation: https://open-meteo.com/en/docs/historical-weather-api
import openmeteo_requests
import click
import pandas as pd
import requests_cache
from retry_requests import retry
import os

@click.command() #function below should be treated as cli command
@click.option('-w', "w", nargs =2, help='Enter begin and end date') # option -d for user
@click.option('--parameters', default=None, is_flag=True, help = "Trigger to get the option of parameters included in dataframe")
@click.option('--newdf', default = None, is_flag = True, help = "Trigger to get an option override dataframe or make a new one")

def get_df(w, parameters, newdf):
    """Input the start and end data of the weather data you want to get. Output is a dataframe of dates, temperatures. """
    if len(w) == 2:
        click.echo(f"Returning data frame from {w[0]} to {w[1]}")
    elif len(w) == 1: 
        # Check how to do this?????
        click.echo(f"You only entered one date. Returning data frame from {w[0]}")
    else:
        click.echo("Please enter a begin and end date")
        raise click.Abort()
    
    start_date = w[0]
    end_date = w[1]
    
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    
    # Mapping the userfriendly names to variable names
    variable_mapping = {
		"temp_real": "temperature_2m",
		"temp_feel": "apparent_temperature",
		"humidity": "relative_humidity_2m",
		"rain": "rain",
		"snowfall": "snowfall",
		"cloud_cover": "cloud_cover",
		"sunshine": "sunshine_duration",
		"wind_speed": "wind_speed_10m",
		"wind_direction": "wind_direction_10m"
	}
    
    if parameters:
        text = input( # userfriendly names of the variables
            "Please enter the variable names you want in the dataframe, separated by commas. \n. The options are:\n "
            "- temp_real: Air temperature at 2 meters above ground\n" 
            "- temp_feel: Perceived feels-like temperature \n"
            "- humidity: Relative humidity at 2 meters above ground\n"
            "- rain: Liquid precipitation of the preceding hour  \n"
            "- snowfall: Snowfall amount of the preceding hour in centimeters.\n"
            "- cloud_cover: Total cloud cover as an area fraction\n" 
            "- sunshine: Number of seconds of sunshine of the preceding hour\n"
            "- wind_speed: Wind speed at 10 meters above ground \n"  
            "- wind_direction: Wind direction at 10 meters above ground\n"
            )
        variable_list = [v.strip() for v in text.split(",")]  # make list + clean the spaces
        
        # Converting user-friendly names back into variable names
        try:
            variable_names = [variable_mapping[v] for v in variable_list]
            hourly_variables = ",".join(variable_names) # make string
        except KeyError as wrong_name: 
            print(f"Invalid variable name: {wrong_name}. Please try again.")
            raise SystemExit()
    
        
    else:
        hourly_variables = "temperature_2m"
    
    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below 
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": hourly_variables
    }
    responses = openmeteo.weather_api(url, params=params)
    
    # Process first location. Add a for-loop for multiple locations or weather models 
    response = responses[0] 
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")
    
    # Process hourly data. 
    # The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    
    hourly_data = {"date": pd.date_range(
		start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
		end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
		freq = pd.Timedelta(seconds = hourly.Interval()),
		inclusive = "left"
	)}	
    for i, var in enumerate(variable_names):
        hourly_data[var] = hourly.Variables(i).ValuesAsNumpy()
    
    # Handling in pandas
    hourly_dataframe = pd.DataFrame(data = hourly_data)
    print(hourly_dataframe)
    
    # Saving to data map
    path = os.path.join(".", "data", "openweathermap")
    os.makedirs(path, exist_ok=True) # if it does not exist yet    
    
    if newdf:
        override = input("Do you want to override the last dataframe (recommended)? Please type Y/ N")
        if override == "Y":
            output_path = os.path.join(path, "weather.csv") # add filename
            click.echo(f"The file name is weather.csv")
            hourly_dataframe.to_csv(output_path, index=False)
        elif override == "N":
            output_path = os.path.join(path, f"weather_{start_date}_to_{end_date}.csv") # add filename
            click.echo(f"The new file name is weather_{start_date}_to_{end_date}.csv")
            hourly_dataframe.to_csv(output_path, index=False)
        else:
            click.echo("Please enter Y or N\n")
        

if __name__=='__main__':
    get_df()