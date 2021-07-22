import os
import boto3
import time
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from s3_md5_compare import md5_compare
from io import BytesIO
import pandas as pd

def source_dataset():
    source_dataset_url = "https://www.ers.usda.gov/webdocs/DataFiles/80591/FoodAccessResearchAtlasData2019.xlsx?v=1848.2"
    sheets = ["Variable Lookup", "Food Access Research Atlas"]

    print(f'Downloading file at {source_dataset_url}')
    response = None
    retries = 5
    for attempt in range(retries):
        try:
            response = urlopen(source_dataset_url)
        except HTTPError as e:
            if attempt == retries:
                raise Exception('HTTPError: ', e.code)
            time.sleep(0.2 * attempt)
        except URLError as e:
            if attempt == retries:
                raise Exception('URLError: ', e.reason)
            time.sleep(0.2 * attempt)
        else:
            break
            
    if response is None:
        raise Exception('There was an issue downloading the dataset')

    filedata = response.read()

    assets = []
    asset_list = []
    data_set_name = os.environ['DATASET_NAME']

    data_dir = '/tmp'
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    s3_bucket = os.environ['ASSET_BUCKET']
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    s3_uploads = []

    for sheet_name in sheets:
        print(f'Sourcing sheet {sheet_name}')
        xl = pd.ExcelFile(filedata, engine="openpyxl")

        if sheet_name not in xl.sheet_names:
            raise Exception(f"required sheet '{sheet_name}' is missing! Aborting...")
        
        sheet_df = xl.parse(sheet_name)
        file_location = os.path.join(data_dir, sheet_name + '.csv')

        sheet_df.to_csv(file_location)

        obj_name = sheet_name.replace(' ', '_').lower()  + '.csv'
        new_s3_key = data_set_name + '/dataset/' + obj_name

        with open(file_location, "rb") as fh:
            has_changes = md5_compare(s3, s3_bucket, new_s3_key, fh)

            if has_changes:
                s3_resource.Object(s3_bucket, new_s3_key).put(Body=fh.read())
                # sys.exit(0)
                print('Uploaded: ' + new_s3_key)
            else:
                print('No changes in: ' + new_s3_key)

        asset_source = {'Bucket': s3_bucket, 'Key': new_s3_key}
        s3_uploads.append({'has_changes': has_changes, 'asset_source': asset_source})

    count_updated_data = sum(upload['has_changes'] == True for upload in s3_uploads)
    if count_updated_data > 0:
        asset_list = list(map(lambda upload: upload['asset_source'], s3_uploads))
        if len(asset_list) == 0:
            raise Exception('Something went wrong when uploading files to s3')

    # asset_list is returned to be used in lamdba_handler function
    # if it is empty, lambda_handler will not republish
    return asset_list

if __name__ == '__main__':
    source_dataset()
