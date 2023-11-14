import io

import boto3
import pandas as pd

from config.base import settings


def create_boto_connection():
    """
    This function creates a S3 connection using boto3.

    :return: botocore.client.S3 or None.
    """
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=(settings.aws_credentials.get('access_key')),
            aws_secret_access_key=(settings.aws_credentials.get('secret_key')),
            aws_session_token=(settings.aws_credentials.get('session_token'))
        )
        return s3_client
    except Exception as e:
        print('Error Creating Boto3 Connection:', e)
        return None


def insert_csv_s3(df: pd.DataFrame, df_target: str, file_name: str) -> None:
    """
    Receive the input DataFrame and use boto3 connection to insert the file into S3 Bucket.

    :param df: (pd.core.frame.DataFrame): The input DataFrame.
    :param df_target: (str): The S3 path/file.
    :param file_name: (str): The name of file been inserted.
    :return: None.
    """
    try:
        s3_client = create_boto_connection()

        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket='data-lake-fast-pb-compassuol', Key=df_target,
                Body=csv_buffer.getvalue()
            )
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful Uploaded {file_name} to S3. Status - {status}")
            else:
                print(f"Unsuccessful Uploaded {file_name} to S3. Status - {status}")
    except Exception as e:
        print('Error Uploading CSV to S3:', e)


def main():
    """
       Read the csv files and transform them into pandas dataframe.

       :return: None.
       """
    try:
        df_target = 'Raw/Local/CSV/Movies/2023/11/11/movies.csv'
        df = pd.read_csv('input/movies.csv', sep='|', dtype={
            'id': str,
            'tituloPincipal': str,
            'tituloOriginal': str,
            'anoLancamento': str,
            'tempoMinutos': str,
            'genero': str,
            'notaMedia': str,
            'numeroVotos': str,
            'generoArtista': str,
            'personagem': str,
            'nomeArtista': str,
            'anoNascimento': str,
            'anoFalecimento': str,
            'profissao': str,
            'titulosMaisConhecidos': str,
        })
        insert_csv_s3(df, df_target, 'movies')

        df_target = 'Raw/Local/CSV/Series/2023/11/11/series.csv'
        df = pd.read_csv('input/series.csv', sep='|', dtype={
            'id': str,
            'tituloPincipal': str,
            'tituloOriginal': str,
            'anoLancamento': str,
            'anoTermino': str,
            'tempoMinutos': str,
            'genero': str,
            'notaMedia': str,
            'numeroVotos': str,
            'generoArtista': str,
            'personagem': str,
            'nomeArtista': str,
            'anoNascimento': str,
            'anoFalecimento': str,
            'profissao': str,
            'titulosMaisConhecidos': str,
        })
        insert_csv_s3(df, df_target, 'series')

        print('Process completed')
    except Exception as e:
        print('Error Uploading CSV:', e)


if __name__ == "__main__":
    main()
