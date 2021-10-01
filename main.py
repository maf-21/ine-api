import requests
from requests.exceptions import HTTPError
import pandas as pd
import os
import glob
import json
from datetime import date

# Define variables for directories and get today's date
script_dir = os.path.dirname(__file__)
extract_dir = os.path.join(script_dir, 'extract')
data_dir = os.path.join(script_dir, 'data')
today = int(date.today().strftime('%Y%m%d'))

# Define indicator to get data and API endpoint. Parameter for first year of data (Dim1) is 'S7A2011'
indicator = '0008074'
reqUrl = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?varcd={indicator}&lang=EN&op=2&Dim1="


def get_parameters_range(reqUrl: str) -> list:
    '''
    Get parameters to query the API, starting from 2011 until last year available of data. 
    This returns a list of parameters with available years to get data in the API
    '''
    try:
        first_year_parameter = 'S7A2011'
        response = requests.get(reqUrl+first_year_parameter)

        if response.status_code == 200:
            print('Acessing the API to get the data range')
            first_year = 2011
            last_year = int(response.json()[0]['UltimoPref'])
            print(f'Last year of data is {last_year}')
            data_range = list(range(first_year, last_year+1))
            print(f'Data from {first_year} until {last_year} will be requested to the API')
            parameters_list = ['S7A' + str(item) for item in data_range]


    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        print('Parameters extracted with success!')

    return parameters_list


def get_raw_data(reqUrl: str, parameters_list: list) -> dict:
    '''
    Query the API for each element (year) in parameters_list, starting from 2011 until last year available.
    Returns a dictionary with data for all available years.
    '''
    data = {}
    for item in parameters_list:
        try:
            response = requests.get(reqUrl+item)
            
            if response.status_code == 200:
                year = item[-4:]
                print(f'Acessing the API to get the data for year {year} ')
                result = response.json()[0]['Dados']
                data.update(result)


        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
        else:
            print(f'Data for year {year} extracted with success!')

    return data


def load_raw_data(raw_data: dict) -> None:
    '''
    Load raw data in '/extract' directory
    '''
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)

    output_file = os.path.join(extract_dir, f'extract-{today}.json')
    with open(output_file, 'w') as f:
        json.dump(raw_data,f)
        print(f'Raw data loaded: {output_file}')

    return None



def transform_raw_data(extract_path: str) -> pd.DataFrame:
    """
    Transform raw data, flattening the nested json in a dataframe, removing unnecessary columns and adding new ones.
    """
    output_file = max(glob.glob(extract_path+"/*"), key=os.path.getmtime)
    print(f'Following file is being transformed: {output_file}')
    with open(output_file) as f:
        raw_data = json.load(f)

    clean_data = []
    
    for item in raw_data.items():
        col = item[0]
        print(f'Transforming data for year: {col}')
        df_flatenned = pd.json_normalize(raw_data, record_path=col)
        df_flatenned['Year'] = col
        clean_data.append(df_flatenned)

    df = pd.concat(clean_data, ignore_index=True)
    df['Indicator Code'] = indicator
    df['Formule'] = '(Number of crimes/ Resident population)*1000'
    df['Measure Of Unit'] = 'Permillage'
    df.drop(columns=['sinal_conv','sinal_conv_desc'], inplace=True)
    df.rename(columns={'geocod': 'Geo Code', 'geodsg': 'Geo', 'dim_3': 'Crime Code', 'dim_3_t': 'Crime', 'valor': 'Value'}, inplace=True)
    return df
    

def load_clean_data(clean_data: pd.DataFrame) -> None:
    """
    Load clean data, in a dataframe format, in '/data' directory.
    This will store the data as a csv file.
    """
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    output_file = os.path.join(data_dir, f'data-{today}.csv')
    clean_data.to_csv(output_file, index=False)
    print(f'Clean data loaded: {output_file}')

    return None



if __name__ == "__main__":

    parameters = get_parameters_range(reqUrl)
    
    raw_data = get_raw_data(reqUrl,parameters)
    load_raw_data(raw_data)

    clean_data = transform_raw_data(extract_dir)
    load_clean_data(clean_data)



