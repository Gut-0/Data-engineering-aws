import boto3
import json
import os
import requests


def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda Function Completed!')
    }


def create_boto_connection():
    """
    This function creates a S3 connection using boto3.

    :return: botocore.client.S3 or None.
    """
    try:
        s3_client = boto3.client('s3')
        return s3_client
    except Exception as e:
        print('Error Creating Boto3 Connection:', e)
        return None


def insert_json_s3(s3_client, json_file: dict, file_name: str):
    """
    Receive the input JSON File and use boto3 connection to insert the file into S3 Bucket.

    :param s3_client: (botocore.client.S3): The boto3 connection.
    :param json_file: (dict): The input json file.
    :param file_name: (str): The name of file been inserted.
    :return: None
    """
    try:
        response = s3_client.put_object(
            Bucket='data-lake-fast-pb-compassuol', Key=f"Raw/TMDB/JSON/Top_Rated_Movies/2023/11/13/{file_name}",
            Body=json.dumps(json_file)
        )
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful Uploaded {file_name} to S3. Status - {status}")
    except Exception as e:
        print('Error Uploading JSON file to S3:', e)


def main():
    """
    Initiate an API request to retrieve the required JSON files.
    :return: None.
    """
    s3_client = create_boto_connection()
    if s3_client is None:
        return

    headers = {
        'accept': 'application/json',
        'Authorization': os.environ['api_key']
    }

    try:
        for i in range(1, 6):
            url = f"https://api.themoviedb.org/3/movie/top_rated?page={i}"
            json_response = requests.get(url, headers=headers).json()
            insert_json_s3(s3_client, json_response, f"top_rated_movies_pt{i}.json")

        print('Process completed.')
    except Exception as e:
        print('Error Getting JSON files:', e)
