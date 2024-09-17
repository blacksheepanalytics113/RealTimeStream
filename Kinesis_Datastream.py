import boto3
import requests
import json
import pandas as pd
import time
from botocore.exceptions import ClientError
from airflow.models import Variable
import numpy as np
def oAuth_example():
    # Step 1: Get the OAuth token
    url = "https://oauth2.bitquery.io/oauth2/token"
    payload = 'grant_type=client_credentials&client_id=041eaf4d-6562-4950-b4ce-4200377ed006&client_secret=HYKTcfLX0mqE--mlmVdQut9IUo&scope=api'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = requests.request("POST", url, headers=headers, data=payload)
    resp = json.loads(response.text)
    access_token = resp['access_token']


    subscription_query = """
    subscription MyQuery {
      EVM(network: eth) {
        Transactions(where: {Transaction: {}, TransactionStatus: {Success: true}}) {
          Transaction {
            From
            Cost
            CostInUSD
            GasPrice
            GasFeeCap
            Gas
            Value
            GasPriceInUSD
          }
        }
      }
    }
    """

    # Step 3: Use the token to send a request for the streaming data
    url_graphql = "https://streaming.bitquery.io/graphql"
    headers_graphql = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    while True:
        try:
            # Send the GraphQL subscription query
            payload_graphql = json.dumps({"query": subscription_query})
            response_data = requests.request("POST", url_graphql, headers=headers_graphql, data=payload_graphql)

            # Parse the streaming response
            resp_data = json.loads(response_data.text)

            # Step 4: Convert response to DataFrame
            transaction_data = resp_data['data']['EVM']['Transactions']

            df = pd.DataFrame([{
                'From': tx['Transaction']['From'],
                'Cost': tx['Transaction']['Cost'],
                'CostInUSD': tx['Transaction']['CostInUSD'],
                'GasPrice': tx['Transaction']['GasPrice'],
                'GasFeeCap': tx['Transaction']['GasFeeCap'],
                'Gas': tx['Transaction']['Gas'],
                'Value': tx['Transaction']['Value'],
                'GasPriceInUSD': tx['Transaction']['GasPriceInUSD']
            } for tx in transaction_data])

            if not df.empty:
                yield df  # Yield data in a streaming fashion
            else:
                time.sleep(1)  # In case of empty data, wait and retry
        except Exception as e:
            print(f"Error fetching data: {e}")
            time.sleep(5)  # Retry after 5 seconds on failure


def send_data_to_kinesis(stream_arn=""):
    """
    Sends data to a Kinesis Data Stream in chunks.

    Args:
        stream_arn (str, optional): The ARN of the Kinesis Data Stream. Defaults to "arn:aws:kinesis:eu-north-1:396913729698:stream/streamdatakinesis".
    """
    access_key_id =  Variable.get("aws_access_key")
    secret_access_key =  Variable.get("aws_access_secret_key")
    region = stream_arn.split(':')[3]

    kinesis_client = boto3.client('kinesis',
                                  aws_access_key_id=access_key_id,
                                  aws_secret_access_key=secret_access_key,
                                  region_name=region)
    stream_name = stream_arn.split('/')[-1]

    for df_data in oAuth_example():
        chunk_size = 1000  
        records = []
        for i in range(0, len(df_data), chunk_size):
            chunk = df_data.iloc[i:i + chunk_size].to_csv(index=False).encode('utf-8')
            records.append({'Data': chunk, 'PartitionKey': 'bitquery_transactions'})
            try:
                response = kinesis_client.put_records(Records=records, StreamName=stream_name)
                print(f"Data sent to Kinesis stream: {response['ResponseMetadata']['HTTPStatusCode']}")
            except ClientError as error:
                print(f"Error sending data to Kinesis: {error}")
                time.sleep(5)  
            records = []
send_data_to_kinesis()
