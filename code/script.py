import json
import pandas as pd
import urllib3
import logging
import psycopg2
import boto3
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

url = "https://jsonplaceholder.typicode.com/users/"
http = urllib3.PoolManager()

def lambda_handler(event, context):
    response = http.request('GET', url)

    # Adding raw json data to first S3 bucket (Extract)
    s3.put_object(
        Bucket='apprentice-training-ml-image-dev-raw',
        Key='etl-raw-data/todo.json',
        Body=response.data
    )
    
    # Data transformation (Transform)
    data = response.data.decode('utf-8')
    json_data = json.loads(data)
    df = pd.DataFrame(json_data)
    
    df.drop(['username', 'company','address'],axis=1, inplace=True)
    df.rename(columns=lambda x: x.upper(), inplace=True)
    
    # Adding cleaned data to second S3 bucket after converting back to json (Load)
    back_to_json = df.to_json(orient='records', indent=2)
    
    s3.put_object(
        Bucket='apprentice-training-ml-image-dev-clean',
        Key='etl-clean-data/todo.json',
        Body=back_to_json.encode('utf-8')
    )
    
    # Insert Data into AWS RDS 
    try:
        conn = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )
        
        cur = conn.cursor()

        # Insert data into the table
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO imageadhikari 
            (ID, NAME, EMAIL, PHONE, WEBSITE)
            VALUES (%s, %s, %s, %s, %s);
            """
            values = (
                row['ID'],
                row['NAME'],
                row['EMAIL'],
                row['PHONE'],
                row['WEBSITE']
            )
            cur.execute(insert_query, values)
            print("successfully executed")
        conn.commit()
        cur.close()

    except psycopg2.Error as e:
        logger.error("Error while inserting data into the database: %s", e)


    finally:
        if conn:
            conn.close()

    # Log successful completion
    logger.info("Data cleaning and storage completed successfully.")

    
    return {
        'statusCode': 200,
        'body': json.dumps('Data cleaning and storage completed successfully.')
    }