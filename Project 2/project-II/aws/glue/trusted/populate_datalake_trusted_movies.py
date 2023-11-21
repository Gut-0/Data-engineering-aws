from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

conf = SparkConf()
conf.setAll(
    [('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem'),
     ('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.3'),
     ('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider'),
     # ('spark.hadoop.fs.s3a.endpoint', 'http://localhost:4566'),
     # ('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false'),
     # ('spark.hadoop.fs.s3a.access.key', 'test'),
     # ('spark.hadoop.fs.s3a.secret.key', 'test'),
     # ('spark.hadoop.fs.s3a.session.token', 'test')
     # ('spark.hadoop.fs.s3a.path.style.access', 'true')
     ])

spark = SparkSession \
    .builder \
    .appName('Glue_read_s3') \
    .config(conf=conf) \
    .getOrCreate()


def read_csv_file(path: str):
    """
    Read a CSV file from S3 and return a Spark dataframe.

    :param path: (str): The path to the CSV file.
    :return: movies_df: (Dataframe): The Spark dataframe.
    """
    try:
        movies_df = spark.read.options(header=True, delimiter='|') \
            .csv(path)
        return movies_df
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None


def save_parquet_s3(df, path: str):
    """
    Save the dataframe as a parquet file to S3.

    :param df: (Dataframe): The dataframe to save.
    :param path: (str): The path to save the dataframe to.
    :return: None
    """
    try:
        df.write.mode("overwrite").parquet(path)
        print("Successfully saved Parquet file into s3")
    except Exception as e:
        print(f"Error saving parquet file: {e}")


def preprocess_movies_df(df):
    """
    Preprocess movies df to remove columns that are not needed for the datalake.
    Also removes rows with null values.
    Also removes duplicates.
    Finally, filters out rows with genre = '\N'.

    :param df: (Dataframe): The input dataframe.
    :return: df: (Dataframe): The filtered dataframe
    """
    try:
        movies_df = df.drop('tituloPincipal', 'tituloOriginal', 'tempoMinutos', 'generoArtista', 'personagem',
                            'nomeArtista',
                            'anoNascimento', 'anoFalecimento', 'profissao', 'titulosMaisConhecidos')
        movies_df = movies_df.na.drop()
        movies_df = movies_df.dropDuplicates().filter(col('genre') != '\\N')
        return movies_df
    except Exception as e:
        print(f"Error preprocessing movies df: {e}")
        return None


if __name__ == '__main__':
    try:
        movies_df = read_csv_file(args['S3_MOVIES_CSV'])
        movies_df = preprocess_movies_df(movies_df)

        save_parquet_s3(movies_df, args['S3_WRITE_PATH'])
    except Exception as e:
        print(f"Error running Glue Job: {e}")
