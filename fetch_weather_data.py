from azureml.core import Workspace, Dataset
from datetime import datetime, timedelta
from typing import NamedTuple
import pytz
import numpy as np
import pandas as pd
from tqdm import tqdm
import pickle as pkl
from config import SUBSCRIPTION_ID, RESOURCE_GROUP, WORKSPACE_NAME

from process_weather import agg_data

from weather_api import get_weather_data

data_path = 'data/output'


def get_point_weather(point: NamedTuple, start: datetime, end: datetime, endpoint: str = 'weather') -> pd.DataFrame:    
    """
    Returns dataframe and quota exceed information

    Parameters:
        - point: pandas dataframe with zipcode as index along with latitude and longitude 
        - start: basic date and time type
        - end: basic date and time type
        - endpoint: string which is either 'weather' or 'bio' or 'solar'
    Returns:
        - returns: pandas dataframe containing datetime and quota exceed which is a boolean
    """        
    lat = point.DPR_lat
    long = point.DPR_long
    end_date = start
    start_date = start
    weather_data = []
    quota_exceed = False

    if endpoint == 'weather':
        quantities = [
            'airTemperature',
            'pressure',
            'cloudCover',
            'humidity',
            'precipitation',
            'snowDepth',
            'windSpeed',
        ]
    elif endpoint == 'bio':
        quantities = [
            'soilMoisture',
            'soilMoisture10cm',
            'soilMoisture40cm',
            'soilMoisture100cm',
            'soilTemperature',
            'soilTemperature10cm',
            'soilTemperature40cm',
            'soilTemperature100cm',
        ]
    elif endpoint == 'solar':
        quantities = [
            'uvIndex',
            'downwardShortWaveRadiationFlux',
        ]

    while start_date < end:
        start_date = end_date

        current_data = get_weather_data(endpoint=f'{endpoint}/point', lat=lat, long=long, quantities=quantities, start_date=start_date)
        weather_data += current_data['hours']
        end_date = datetime.strptime(current_data['meta']['end'], '%Y-%m-%d %H:%M') + timedelta(hours=1)
        if current_data["meta"]["requestCount"] % 5000 == 0:
            print(f"Used {current_data['meta']['requestCount']} calls.")
        if current_data["meta"]["requestCount"] == current_data["meta"]["dailyQuota"]:
            quota_exceed = True
            print(f"Daily Quota Exhausted: You may start again tomorrow with the parameters:\n{start_date=}, {end_date=}")
            start_date = end
    #creating the weather_df dataframe
    weather_df = pd.DataFrame([{key: (value if key == 'time' else [value[k] for k in value][0]) for key, value in entry.items()} for entry in weather_data])
    try:
        weather_df['time'] = pd.to_datetime(weather_df['time'])
    except KeyError:
        pass
    return weather_df, quota_exceed

def generate_data(top_coords_unique: pd.DataFrame, endpoint: str, years:int) -> dict:
    
    """
    Returns pickle file that will be used to generate csv file in process_weather.py

    Parameters:
        - top_coords_unique: Pandas dataframe 
        - endpoint: string which is either 'weather' or 'bio' or 'solar'
        - year: integar  
    Returns:
        - returns: Pickle file that will be used to generate a csv file
    """
    for year in years:
        weather_dict = {}
        full_weather_dict = {}
        for row in tqdm(top_coords_unique.itertuples(), total=top_coords_unique.shape[0]):
            start = datetime(year, 1, 1)
            if(year == 2022): end = datetime(year, 10, 1) #  end collecting data on 1 oct 2022
            else: end = datetime(year + 1, 1, 1) #end collecting data on Jan 1 of next year
            try:        
                weather_df, quota_exceeded = get_point_weather(row, start, end, endpoint=endpoint)
                #use above function to get weather_df and quota_exceeded  
                if not quota_exceeded:
                    if weather_df.shape[0] == 0:
                        print(f"No Data for {row.Index}")
                    else:
                        full_weather_dict[row.Index] = weather_df
                else:
                    print(f'Stopping at {row} because of quota.')
                    break
            except:
                print("Gateway Timeout...", row.Index)
        pkl.dump(full_weather_dict, open(f'{data_path}{year}/full_{endpoint}_data_top100.pkl', 'wb'))
        aggregated_data = agg_data(full_weather_dict, year, endpoint, data_path)
        print({endpoint} 'data shape for' {year}, aggregated_data.shape)

def main():

    workspace = Workspace(SUBSCRIPTION_ID, RESOURCE_GROUP, WORKSPACE_NAME)

    # Load PCA
    PCA = Dataset.get_by_name(workspace, name='PCA') #accessing data
    pca_df = PCA.to_pandas_dataframe()  #converting it into pandas dataframe

    # Check years in dataset
    pca_df['DPR_Issued or Renewed Date'] = pd.to_datetime(pca_df['DPR_Issued or Renewed Date'])
    pca_df['Year'] = pca_df['DPR_Issued or Renewed Date'].dt.year
    pca_df[['DPR_lat', 'DPR_long']] = pca_df[['DPR_lat', 'DPR_long']].astype(float) #converting it to float datatype

    # Create df of just zip codes and coordinates, drop NA values and set zip code as index
    coords = pca_df[['DPR_Zip', 'DPR_lat', 'DPR_long']].dropna()

    #Normalizing the value_counts of the zipcoes and consing only the top 125 most used zip codes
    top_codes = coords['DPR_Zip'].value_counts(normalize=True).head(125).index

    top_coords = coords[coords['DPR_Zip'].isin(top_codes)] #considering only those zipcodes that are in the top 125 zip codes

    top_coords_unique = top_coords.groupby('DPR_Zip').first()#Grouping by zip codes

    endpoints = ['bio', 'weather', 'solar']
    for endpoint in endpoints:
        if endpoint == 'weather':
            years = [2019, 2020, 2021, 2022] #weather data is available for 2019 unlike 'solar' and 'bio'
        else:
            years = [2020, 2021, 2022]

        generate_data(top_coords_unique, endpoint, years)

if __name__=="__main__":
    main()

