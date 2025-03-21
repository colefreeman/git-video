import io
import pandas as pd
import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_live_tournament_stats(*args, **kwargs):
    """
    Load live tournament stats from the DataGolf API and return a DataFrame.
    """
    api_key = get_secret_value("DATAGOLF_API_KEY")
    url = 'https://feeds.datagolf.com/preds/live-tournament-stats'
    
    all_available_stats = [
        'sg_putt', 'sg_arg', 'sg_app', 'sg_ott', 'sg_t2g', 'sg_bs', 'sg_total', 
        'distance', 'accuracy', 'gir', 'prox_fw', 'prox_rgh', 'scrambling'
    ]
    
    params = {
        'key': api_key,
        'file_format': 'csv',
        'display': kwargs.get('display', 'value'),
        'stats': kwargs.get('stats', ','.join(all_available_stats))
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text))
        
        # Ensure the dataframe columns match the expected schema
        expected_columns = [
            'event_name', 'last_updated', 'stat_display', 'position', 'player_name', 
            'dg_id', 'stat_round', 'course', 'total', 'round', 'thru', 'sg_putt', 
            'sg_arg', 'sg_app', 'sg_ott', 'sg_t2g', 'sg_bs', 'sg_total', 'distance', 
            'accuracy', 'gir', 'prox_fw', 'prox_rgh', 'scrambling'
        ]
        
        # Adding missing columns with NaN values if not present in the response
        for col in expected_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Reorder the columns to match the expected schema
        df = df[expected_columns]
        
        # Cast columns to the correct data types
        df['event_name'] = df['event_name'].astype('string')
        df['last_updated'] = df['last_updated'].astype('string')
        df['stat_display'] = df['stat_display'].astype('string')
        df['position'] = df['position'].astype('string')
        df['player_name'] = df['player_name'].astype('string')
        df['dg_id'] = pd.to_numeric(df['dg_id'], errors='coerce')
        df['stat_round'] = pd.to_numeric(df['stat_round'], errors='coerce')
        df['course'] = df['course'].astype('string')
        df['total'] = pd.to_numeric(df['total'], errors='coerce')
        df['round'] = pd.to_numeric(df['round'], errors='coerce')
        df['thru'] = pd.to_numeric(df['thru'], errors='coerce')
        df['sg_putt'] = pd.to_numeric(df['sg_putt'], errors='coerce')
        df['sg_arg'] = pd.to_numeric(df['sg_arg'], errors='coerce')
        df['sg_app'] = pd.to_numeric(df['sg_app'], errors='coerce')
        df['sg_ott'] = pd.to_numeric(df['sg_ott'], errors='coerce')
        df['sg_t2g'] = pd.to_numeric(df['sg_t2g'], errors='coerce')
        df['sg_bs'] = pd.to_numeric(df['sg_bs'], errors='coerce')
        df['sg_total'] = pd.to_numeric(df['sg_total'], errors='coerce')
        df['distance'] = pd.to_numeric(df['distance'], errors='coerce')
        df['accuracy'] = pd.to_numeric(df['accuracy'], errors='coerce')
        df['gir'] = pd.to_numeric(df['gir'], errors='coerce')
        df['prox_fw'] = pd.to_numeric(df['prox_fw'], errors='coerce')
        df['prox_rgh'] = pd.to_numeric(df['prox_rgh'], errors='coerce')
        df['scrambling'] = pd.to_numeric(df['scrambling'], errors='coerce')
        
        # Debugging: Print column names and check for nulls
        print("Columns in DataFrame:", df.columns)
        print("Null values per column before fill:")
        print(df[['event_name', 'last_updated', 'stat_display']].isnull().sum())
        
        # Fill missing values for key columns
        df['event_name'].fillna(method='ffill', inplace=True)
        df['last_updated'].fillna(method='ffill', inplace=True)
        df['stat_display'].fillna(method='ffill', inplace=True)
        
        print("Null values per column after fill:")
        print(df[['event_name', 'last_updated', 'stat_display']].isnull().sum())
        
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching live tournament stats: {e}")
        return pd.DataFrame()