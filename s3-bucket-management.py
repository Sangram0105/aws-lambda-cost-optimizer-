import boto3
import datetime
import time

s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    bucket_name = 'lambdausecase'
    minutes_threshold = 5  # Number of minutes after which to change storage class 
                           #The 5 min time is taken   for testing purposes
    current_time = datetime.datetime.now()
    
    # The total number of object in the bucket is calculated using this function
    objects = s3.list_objects_v2(Bucket=bucket_name)
    
    if 'Contents' in objects:
        for obj in objects['Contents']:
            # Calculate the object's age in minutes 
               last_modified = obj['LastModified']
               object_age_minutes = (current_time - last_modified.replace(tzinfo=None)).total_seconds() / 60
               
               if object_age_minutes > minutes_threshold:
                   # Change the storage class to GLACIER for testing purposes
                   #because the class glacier is the cheapest storage class
                   s3.copy_object(
                       Bucket=bucket_name,
                       Key=obj['Key'],
                       CopySource={'Bucket': bucket_name, 'Key': obj['Key']},
                       StorageClass='GLACIER',
                       MetadataDirective='COPY'
                   )
                   print(f"Changed storage class for {obj['Key']} to GLACIER")
       
       # The report of the lambda function is generated and stored in the s3 bucket
        report_content = f"Report generated at {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
        file_name = f"report_{current_time.strftime('%Y%m%d')}.txt"
       
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=report_content)
       
       # Check if current time is before the end time (e.g., 8:00 PM)
        end_time = current_time.replace(hour=20, minute=0, second=0, microsecond=0)
        if current_time < end_time:
           # Re-invoke this function after 5 minutes
           lambda_client.invoke(
               FunctionName=context.invoked_function_arn,
               InvocationType='Event',
               Payload='{}'
           )
       
        return {
           'statusCode': 200,
           'body': f'Report generated and stored in {bucket_name}/{file_name}'
       }

           
