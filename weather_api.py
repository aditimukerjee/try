from datetime import datetime
from typing import List
import requests
import structlog
import json

with open('keys.json', 'r', encoding='utf-8') as f:
    env_keys = json.load(f)

# Stormglass.io API
STORMGLASS_API = env_keys['STORMGLASS_API_KEY']

log = structlog.get_logger()


def get_headers() -> dict:
    headers = {
        'Authorization': STORMGLASS_API
    }
    return headers


def get_weather_data(endpoint: str, lat: float, long: float, quantities: List[str], start_date: datetime = None, end_date: datetime = None) -> json:
        """
    Returns response

    Parameters:
        - endpoint: string which is either 'weather' or 'bio' or 'solar' 
        - lat: latitude which is a float
        - long: longitude which is a float  
        - quantities: list of strings
        - start_date: basic date and time type
        - end_date: basic date and time type
    Returns:
        - returns: response
    """        
    url = f'https://api.stormglass.io/v2/{endpoint}'
    start = datetime.timestamp(start_date) if start_date else None
    end = datetime.timestamp(end_date) if end_date else None

    parameters = {
        'lat': lat,
        'lng': long,
        'params': ','.join(quantities),
        'start': start,
        'end': end,
        'source': 'noaa',
    }

    response = requests.get(url, params=parameters, headers=get_headers())
    if response.status_code < 400:
        return response.json()
    else:
        log.error(f'Error in request: {response.status_code}. \n{response.reason}')
        return response