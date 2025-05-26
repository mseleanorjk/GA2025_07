"""
Tool for handling OpenWeatherMap API requests and data files

Choices of the user:
- the parameters wanted for in the dataframe 
- whether the dataframe will be overwritten or saved separately
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

# Mapping the user-friendly names to variable names
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

# Define function that gathers data from openweather, can be called outside cli 
def get_weather_df(start_date, end_date, variable_list=None, overwrite=False):
    
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    if variable_list:
        try:
            variable_names = [variable_mapping[v] for v in variable_list]
            hourly_variables = ",".join(variable_names) # make string of all variables together
        except KeyError as wrong_name: 
            print(f"Invalid variable name: {wrong_name}. Please try again.")
            raise SystemExit()
    else:
        variable_names = ["temperature_2m"]
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
    os.makedirs(path, exist_ok=True) # if it does not exist yet --> make the map   
    
    # getting user input whether to override dataframe 
            #click.echo("IPlease enter Y or N\n")
    # no input of newdf --> saved as weather.csv
    
    if overwrite:
        output_path = os.path.join(path, "weather.csv")
        hourly_dataframe.to_csv(output_path, index=False)
    else: 
        output_path = os.path.join(path, f"weather_{start_date}_to_{end_date}.csv") 
        hourly_dataframe.to_csv(output_path, index=False)


@click.command() #function below should be treated as cli command
#@click.option('-w', "w", nargs =2, help='Enter begin and end date') # option -w for user, makes sure that we can enter two inputs
@click.option('--parameters', default=None, is_flag=True, help = "Trigger to get the option of parameters included in dataframe")
@click.option('--newdf', default = None, is_flag = True, help = "Trigger to get an option override dataframe or make a new one")
@click.argument("w", nargs =-1, required = True) # makes sure that 1 argument is also possible


#CLI tool that gathers parameters 
def get_cli_df(w, parameters, newdf):
    """Input the start and end data of the weather data you want to get (w) + parameters + overwrite y/n. 
    Output is a dataframe of dates, and selected variables. """
    if len(w) == 2: 
        click.echo(f"Returning data frame from {w[0]} to {w[1]}")
        # defining the dates
        start_date = w[0]
        end_date = w[1]
    
    elif len(w) == 1: 
        click.echo(f"You only entered one date. Returning data frame from {w[0]}")
        # defining the dates
        start_date = w[0]
        end_date = w[0]
    
    else: # wrong entry
        click.echo("Please enter one begin date and one end date")
        raise click.Abort()
    
    # getting user input for the parameters to include in dataframe
    if parameters:
        text = input( # user-friendly names of the variables
            "Please enter the variable names you want in the dataframe, separated by commas.\n The options are:\n "
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
        variable_list = [v.strip() for v in text.split(",")] 
        if len(variable_list)==1:
            if not variable_list[0]: 
                variable_list = None    
    else:
        variable_list= None

        
    if newdf:
        override = input("Do you want to override the last dataframe (recommended)? Please type Y/ N.\n")
        if (override == "Y" or override == "y"):
            overwrite= "y"
            click.echo(f"The dataframe is saved as weather.csv")

        elif (override == "N" or override == "n"):
            overwrite=None
            
            click.echo(f"The new file is saved as weather_{start_date}_to_{end_date}.csv")

        elif KeyError: 
            print("Invalid answer. Please try again and enter either Y or N.")
            raise SystemExit()
        
    get_weather_df(start_date, end_date, variable_list=variable_list, overwrite=overwrite)


if __name__=='__main__':
    get_cli_df()