import os
from io import StringIO

import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pyproj import Transformer
from sodapy import Socrata


def get_results(date, client):
    results = client.get('wjz9-h9np',
                         where=f'issue_date>="{date}" and \
                            latitude!=99999 and \
                            longitude!=99999 and \
                            location!="TEST"',
                         limit=50000)
    results_df = pd.DataFrame.from_records(results)
    return results_df


def parse_time(time):
    # time is formatted as an integer; e.g. 930 is 9:30 a.m., 1730 is 5:30 p.m.
    if len(time) == 3:
        return '0' + time[0] + ':' + time[-2:]
    else:
        return time[0:-2] + ':' + time[-2:]


def convert_coord(lat, long):
    transformer = Transformer.from_crs('epsg:2229', 'epsg:4326')
    lat_new, long_new = transformer.transform(lat, long)
    return lat_new, long_new


def main():
    load_dotenv()
    CONNECT_STR = os.getenv('CONNECTION_STRING')

    # Fetch and process data
    client = Socrata('data.lacity.org', None)
    df = get_results('2020-08-01', client)
    df = df[pd.notna(df.violation_description)].reset_index(drop=True)

    df['issue_date'] = [datetime[0:10] for datetime in df.issue_date]
    df['issue_time'] = [parse_time(time) for time in df.issue_time]

    lat_new, long_new = convert_coord(df['latitude'].values, df['longitude'].values)
    df['latitude'] = lat_new
    df['longitude'] = long_new

    # Write to Azure blob
    blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
    container_name = 'lacity-citations'
    blob_service_client.create_container(container_name)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob='citations.csv')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)


main()
