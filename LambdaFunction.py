import os
import csv
import json
import boto3
import uuid

"""
  lambda trigger function
  function will be executed once new file is added to s3 bucket
"""
def lambda_handler(event,context):
    
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        
        resource = boto3.resource('s3')
    
        if event:
        
            file_object = event["Records"][0]
            file_name = str(file_object['s3']['object']['key'])
        
            if file_name:
            
               resource.Bucket(bucket).download_file(file_name, "~/" + file_name)
               
    except Exception as e:
        
      print(e)
