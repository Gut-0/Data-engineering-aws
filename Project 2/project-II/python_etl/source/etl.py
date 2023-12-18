import pandas as pd

from config.base import settings


def get_aws_credentials():
    """
    The code uses S3FS filesystem interface for S3.

    :return: dict or None.
    """
    try:
        aws_credentials = {
            "key": settings.aws_credentials.get('access_key'),
            "secret": settings.aws_credentials.get('secret_key'),
            "token": settings.aws_credentials.get('session_token')
        }
        return aws_credentials
    except Exception as e:
        print('Error Getting Connection:', e)
        return None


def insert_csv_s3(df: pd.DataFrame, df_target: str, file_name: str) -> None:
    """
    Receive the input DataFrame and use 'to_csv' method to insert the file into S3 Bucket.

    :param df: (pd.core.frame.DataFrame): The input DataFrame.
    :param df_target: (str): The S3 path/file.
    :param file_name: (str): The name of file been inserted.
    :return: None.
    """
    try:
        aws_credentials = get_aws_credentials()
        df.to_csv(df_target, sep='|', index=False, encoding='UTF-8', storage_options=aws_credentials)

        print(f"Successful Uploaded {file_name} to S3. Status - 200")
    except Exception as e:
        print('Error Uploading CSV to S3:', e)


def main():
    """
       Read the csv files and transform them into pandas dataframe.

       :return: None.
       """
    try:
        df_target = 's3a://data-lake-fast-pb-compassuol/Raw/Local/CSV/Movies/2023/11/15/movies.csv'
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

        df_target = 's3a://data-lake-fast-pb-compassuol/Raw/Local/CSV/Series/2023/11/15/series.csv'
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
