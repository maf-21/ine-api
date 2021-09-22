import requests
from requests.exceptions import HTTPError
import pandas as pd
from pandas import json_normalize
import os
import glob
import json
from datetime import date


script_dir = os.path.dirname(__file__)
extract_dir = os.path.join(script_dir, 'extract')
data_dir = os.path.join(script_dir, 'data')
today = int(date.today().strftime('%Y%m%d'))


indicator = '0008074'


reqUrl = f"https://www.ine.pt/ine/json_indicador/pindica.jsp?varcd={indicator}&lang=EN&op=2&Dim1="


def get_parameters_range(reqUrl):
    '''
    Get parameters to query the API, starting from 2011 until last year available of data
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


def get_raw_data(reqUrl, parameters_list):
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


def load_raw_data(raw_data):

    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)

    output_file = os.path.join(extract_dir, f'extract-{today}.json')
    with open(output_file, 'w') as f:
        json.dump(raw_data,f)
        print(f'Extraction loaded in extract folder')



def transform_raw_data(extract_path):
    output_file = max(glob.glob(extract_path+"/*"), key=os.path.getmtime)
    print(f'Following file is being transformed: {output_file}')
    with open(output_file) as f:
        raw_data = json.load(f)

    clean_data = []
    
    for item in raw_data.items():
        col = item[0]
        print(f'Transforming data for year: {col}')
        df_flatenned = json_normalize(raw_data, record_path=col)
        df_flatenned['Year'] = col
        clean_data.append(df_flatenned)

    df = pd.concat(clean_data, ignore_index=True)
    df['Indicator Code'] = indicator
    df['Formule'] = '(Number of crimes/ Resident population)*1000'
    df['Measure Of Unit'] = 'Permillage'
    df.drop(columns=['sinal_conv','sinal_conv_desc'], inplace=True)
    df.rename(columns={'geocod': 'Geo Code', 'geodsg': 'Geo', 'dim_3': 'Crime Code', 'dim_3_t': 'Crime', 'valor': 'Value'}, inplace=True)
    return df
    

def load_clean_data(clean_data):

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    output_file = os.path.join(data_dir, f'data-{today}.csv')
    clean_data.to_csv(output_file, index=False)
    print('Clean data loaded in data folder')



if __name__ == "__main__":

    parameters = get_parameters_range(reqUrl)
    raw_data = get_raw_data(reqUrl,parameters)

    load_raw_data(raw_data)

    clean_data = transform_raw_data(extract_dir)

    load_clean_data(clean_data)



