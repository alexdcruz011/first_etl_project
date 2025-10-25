import requests
import json
import pandas as pd
from dotenv import load_dotenv
import os
import pandera.pandas as pa
from pandera import Column, DataFrameSchema
from io import StringIO
from google_auth_oauthlib.flow import InstalledAppFlow
from google.cloud import storage
from google.oauth2.credentials import Credentials 
from google.auth.transport.requests import Request
import google.auth
import gcsfs


def extract_cc_data(api_key: str, gcp_path : str):
    """Extracts credit card data from Mockaroo API using the provided API key
    Returns a Pandas Dataframe
    """
    params = { "key" : api_key}
    headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {params.get("key")}'
    }

    response = requests.get('https://my.api.mockaroo.com/sample_cc_data_alex.json',headers=headers, params=params)

    if response.status_code != 200:
        raise RuntimeError(f"API Extraction failed status code {response.status_code}")

    json_string = json.dumps(response.json())
    json_object = StringIO(json_string)
    data = pd.read_json(json_object)
    credentials, project = google.auth.default()
    fs = gcsfs.GCSFileSystem(token=credentials)
    with fs.open(gcp_path, 'w') as f:
        data.to_csv(f, index=True)



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


def load_cc_data(df, dest_path):
    df.to_csv(dest_path , index = False)
    print(f'Load completed. Find {dest_path}')

def get_gcp_creds():
    scopes=['https://www.googleapis.com/auth/devstorage.read_write']

    creds = None
    if os.path.exists("token_main.json"):
        creds = Credentials.from_authorized_user_file("token_main.json", scopes)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret.json', scopes
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token_main.json", "w") as token:
            token.write(creds.to_json())
    return creds

def upload_to_gcs(file, creds, project, bucket_name, blob_name):
    client = storage.Client(project=project, credentials=creds)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file)

def extract_gcsfs():
    credentials, project = google.auth.default()
    fs = gcsfs.GCSFileSystem(token=credentials)
    with fs.open('gs://raw_data_alex_portfolio/raw_data/raw_output_cc_alex.csv') as f:
        df = pd.read_csv(f)
    return df

if __name__ == '__main__':
    load_dotenv()
    api_key = os.getenv('API_KEY')
    project = os.getenv('PROJECT_NAME')
    bucket_name = os.getenv('BUCKET_NAME')
    dest_blob_path = 'raw_data/output_cc_alex.csv'
    raw_blob_path = 'raw_data/raw_output_cc_alex.csv'
    raw_path = 'raw_output_cc_data.csv'
    dest_path = 'output_cc_data.csv'
    extract_cc_data(api_key)
    creds = get_gcp_creds()
    upload_to_gcs(file=raw_path, creds=creds, project=project, bucket_name=bucket_name, blob_name=raw_blob_path)
    initial_df = extract_gcsfs()
    print(initial_df)
    final = transform_cc_data(initial_df)
    load_cc_data(final,dest_path)
    upload_to_gcs(file=dest_path, creds=creds, project=project, bucket_name=bucket_name, blob_name=dest_blob_path)