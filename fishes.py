import boto3
import numpy as np
import pandas as pd
import pprint as pp

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
bucket_list = s3_client.list_buckets()
bucket_name = 'data-eng-resources'
bucket_contents = s3_client.list_objects(Bucket=bucket_name, Prefix='python/fish-market')
bucket_files = bucket_contents['Contents']
df_list = []
pp.pprint(bucket_contents)


def readBucket():

    for content in bucket_files:
        data_object = s3_client.get_object(Bucket=bucket_name, Key=content['Key'])
        data_df = pd.read_csv(data_object['Body'])
        df_list.append(data_df)
    return df_list

def check_bucket_average(desired_parameter='Species', file_name='averages.csv'):
    df = pd.concat(df_list)
    df = df.reset_index()
    df = df.drop('index', axis=1 )
    df_average = df.groupby([desired_parameter]).mean()
    print(df_average)
    df_average.to_csv(file_name, mode='a', header=True)

def uploader(file_name= 'averages.csv'):
    s3_client.upload_file(Filename= file_name,
                          Bucket= bucket_name,
                          Key ='Data30/Mateus/averages.csv')
def doAllInOne():
    readBucket()
    check_bucket_average()
    uploader()

doAllInOne()