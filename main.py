import boto3
import os
import pandas as pd
import psycopg2 as psql
import warnings
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=UserWarning)

load_dotenv()

local_file_path = 'datasets/customer_personal_info.csv'
file_name = 'customer_personal_info'
s3_path = os.getenv("S3_PATH")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
aws_topic_arn = os.getenv('TOPIC_ARN')
pg_host = os.getenv('PG_HOST')
pg_dbname = os.getenv('PG_DBNAME')
pg_user = os.getenv('PG_USER')
pg_password = os.getenv('PG_PASSWORD')
pg_port = os.getenv('PG_PORT')


def extract_csv_from_psql(csv_path, host, dbname, user, password, port):
    load_dotenv()
    conn = None
    cur = None
    try:
        conn = psql.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port
        )
        cur = conn.cursor()
        sql_query = 'select * from shopping_order_reporting.customer_personal_info;'
        data = pd.read_sql_query(sql_query, conn)
        data.to_csv(f'{csv_path}', index=False)
        print('File Extraction: SUCCESS')

    except Exception as connection_error:
        return connection_error

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def upload_file_to_s3(file_path, bucket_name, bucket_path):
    try:
        s3_client = boto3.client('s3')
        with open(file_path, 'rb') as f:
            s3_client.upload_fileobj(f, bucket_name, bucket_path)
        print('File s3 Load: SUCCESS')
        return True

    except Exception as load_error:
        print(load_error)
        return False


# notifying the customers on the file upload
def successful_upload_email(topic_arn):
    try:
        sns_client = boto3.client('sns')

        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=f'''
        Dear Customer,
        
        Your {file_name} has been successfully extracted from {pg_dbname} database and uploaded to this {s3_bucket_name} bucket.
        
        Best,
        tech-support@companyx.com
        
        ''',
            Subject='Hooray! Successful File Upload',
        )
        print(f"SNS publish status code:{response['ResponseMetadata']['HTTPStatusCode']}")
        return True
    except Exception as message_error:
        print(f'Failed to send SNS message due to {message_error}')
        return False


# notifying the customers on the issue received
def unsuccessful_upload_email(topic_arn):
    try:
        sns_client = boto3.client('sns')

        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=f'''
        Dear Customer,

        We have encountered an issue while trying to upload this {file_name} from {pg_dbname} database and 
        put the file to this {s3_bucket_name} bucket. We are currently working on this and will provide update by 
        end of day.

        Best,
        tech-support@companyx.com

        ''',
            Subject='Issue with File Upload',
        )
        print(f"SNS publish status code:{response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as message_error:
        print(f'Failed to send SNS message due to {message_error}')


# Finally calling the function

if extract_csv_from_psql(csv_path=local_file_path, host=pg_host,
                         dbname=pg_dbname, user=pg_user,
                         password=pg_password, port=pg_port):

    if upload_file_to_s3(bucket_path=s3_path, bucket_name=s3_bucket_name, file_path=local_file_path):
        successful_upload_email(topic_arn=aws_topic_arn)
    else:
        unsuccessful_upload_email(topic_arn=aws_topic_arn)
else:
    unsuccessful_upload_email(topic_arn=aws_topic_arn)
