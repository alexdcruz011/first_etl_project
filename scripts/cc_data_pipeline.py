import requests
import json
import pandas as pd
from dotenv import load_dotenv
import os
import pandera.pandas as pa
from pandera import Column, DataFrameSchema
from io import StringIO


def extract_cc_data(api_key: str):
    params = { "key" : api_key}
    headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {params.get("key")}' # If your Mockaroo API requires authentication
    }

    response = requests.get('https://my.api.mockaroo.com/sample_cc_data_alex.json',headers=headers, params=params)

    if response.status_code != 200:
        print(f"API Extraction failed status code {response.status_code}")

    json_string = json.dumps(response.json())
    json_object = StringIO(json_string)

    return pd.read_json(json_object)


def transform_cc_data(initial_df):
    schema = DataFrameSchema({
        'first_name' : Column(str),
        'last_name' : Column(str),
        'email' : Column(str),
        'gender' : Column(str),
        'credit_card_number' : Column(str),
        'mobile_number' : Column(str,nullable=True),
        'mailing_address' : Column(str,nullable=True),
        'ssn' : Column(str,nullable=True),
        'date_of_birth' : Column(pa.DateTime,nullable=True)
    })

    df = initial_df

    df['first_name'] = df['first_name'].str.capitalize()
    df['last_name'] = df['last_name'].str.capitalize()
    df['email'] = df['email'].fillna('no_email@email.com')
    df['remaining_balance'] = df['remaining_balance'].astype(float)
    df['ssn'] = df['ssn'].str[-4:]
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
    df['gender'] = df['gender'].fillna('Not Specified')
    df['credit_card_number'] = df['credit_card_number'].astype(str)

    df.rename(columns={'remaining_balance':'credit_limit'},inplace=True)
    test = schema.validate(df)
    return df


def load_cc_data(df):
    dest_path = 'output_cc_data.csv'
    df.to_csv(dest_path , index = False)
    print(f'Load completed. Find {dest_path}')


if __name__ == '__main__':
    load_dotenv()
    api_key = os.getenv('API_KEY')
    initial_df=extract_cc_data(api_key)
    final = transform_cc_data(initial_df)
    load_cc_data(final)