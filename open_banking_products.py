import requests
import pandas as pd
import boto3
import json
from pandas.io.json import json_normalize
from datetime import datetime

def get_bank_name(url):

    if url.find("nab") != -1:
        bank_name = "nab"
    elif url.find("anz") != -1:
        bank_name = "anz"
    elif url.find("westpac") != -1:
        bank_name = "westpac"
    elif url.find("commbank") != -1:
        bank_name = "commbank"
    
    return bank_name
    
def lambda_handler(event, context):
    headers = {"x-v":"1"}
    params = {"page-size":"100"}
    products_urls = ["https://api.anz/cds-au/v1/banking/products","https://openbank.api.nab.com.au/cds-au/v1/banking/products","https://digital-api.westpac.com.au/cds-au/v1/banking/products","https://api.commbank.com.au/public/cds-au/v1/banking/products"]
    raw_bucket = "raw-openbanking"
    appended_data = pd.DataFrame()

    currentYear = str(datetime.now().year)
    if len(str(datetime.now().month)) == 1:
        currentMonth = '0' + str(datetime.now().month)
    else:
        currentMonth = str(datetime.now().month)
        
    if len(str(datetime.now().day)) == 1:
        currentDay = '0' + str(datetime.now().day)
    else:
        currentDay = str(datetime.now().day)
    


    for url in products_urls:
        response = requests.get(url=url,headers=headers,params=params)
        json_response = response.json()
        products = json_normalize(json_response['data'], 'products')
        s3_key = get_bank_name(url) + '/year=' + currentYear + '/month=' + currentMonth + '/day=' + currentDay + '/'
    
        for index,row in products.iterrows():
            product_id = row['productId']
            details_url = url + '/' + product_id
            details_response = requests.get(url=details_url,headers=headers)
            response_details = details_response.json()

            s3 = boto3.resource('s3')
            obj = s3.Object(raw_bucket, s3_key + product_id + ".json")
            obj.put(Body=json.dumps(response_details))
