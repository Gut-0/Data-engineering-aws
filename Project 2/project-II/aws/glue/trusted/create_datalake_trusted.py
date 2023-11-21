from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, round, year, collect_list, concat_ws

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


def read_json_file(path: str):
    """
    Read a JSON file from S3 and return a Spark dataframe.

    :param path: (str): The path to the JSON file.
    :return: json_df: (Dataframe): The Spark dataframe.
    """
    try:
        json_df = spark.read.json(path)
        return json_df
    except Exception as e:
        print(f"Error reading JSON file: {e}")
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
    Preprocesses a PySpark DataFrame containing movie information.
    Extracts 'results' column and flattens it
    Creates 'movies_df' with movie ID, release year, genre ID, vote average, and count

    :param df: (DataFrame): the input dataframe.
    :return: movies_df: (DataFrame): the preprocessed dataframe.
    """
    try:
        df = df.drop('page', 'total_pages', 'total_results')
        df = [df.select(explode("results").alias("result")) for column in df]

        movies_df = df[0].select("result.id", "result.release_date", "result.genre_ids", "result.vote_average",
                                 "result.vote_count")

        movies_df = movies_df.withColumnRenamed("id", "movie_id")
        movies_df = movies_df.withColumn("vote_average", round(col("vote_average"), 1))
        movies_df = movies_df.withColumn("release_year", year(col("release_date"))).drop("release_date")
        movies_df = movies_df.withColumn("genre_id", explode("genre_ids")).drop("genre_ids")

        return movies_df
    except Exception as e:
        print(f"Error preprocessing movies df: {e}")


def preprocess_genres_df(genres_df):
    """
    Preprocess the genres df and  explode the genres' column.
    The genres column is a list of dictionaries, so we need to explode it to get the individual genres.
    The exploded genres column is a dictionary with the id and name of the genre, so we need to select the id and name columns.

    :param genres_df: (DataFrame): the genres dataframe.
    :return: genres_df: (DataFrame): the preprocessed genres dataframe.
    """
    try:
        genres_df = genres_df.withColumn('genre', explode('genres')).drop('genres')
        genres_df = genres_df.select(col("genre.id").alias("id"), col("genre.name").alias("name"))

        return genres_df
    except Exception as e:
        print(f"Error preprocessing genres df: {e}")
        return None


def preprocess_ratings_df(movies_df, genres_df):
    """
    Preprocess the ratings df by joining the movies and genres df on the genre_id column.
    Then, group the ratings df by movie_id and concatenate the genres into a single string separated by commas.

    :param movies_df: (DataFrame): the movies dataframe.
    :param genres_df: (DataFrame): the genres dataframe.
    :return: ratings_df: (DataFrame): the processed ratings dataframe.
    """
    try:
        merged_df = movies_df.join(genres_df, movies_df.genre_id == genres_df.id, "left")
        ratings_df = merged_df.select("movie_id", "vote_average", "vote_count", genres_df.name.alias("genre_name"),
                                      "release_year")

        grouped_df = ratings_df.groupBy("movie_id", "vote_average", "vote_count", "release_year").agg(
            collect_list("genre_name").alias("genre"))
        ratings_df = grouped_df.withColumn("genre", concat_ws(",", "genre"))

        return ratings_df
    except Exception as e:
        print(f"Error preprocessing ratings df: {e}")
        return None


if __name__ == '__main__':
    try:
        genres_df = read_json_file(args['S3_GENRES_JSON'])
        movies_df = read_json_file(args['S3_MOVIES_JSON'])

        movies_df = preprocess_movies_df(movies_df)
        genres_df = preprocess_genres_df(genres_df)

        ratings_df = preprocess_ratings_df(movies_df, genres_df)
        save_parquet_s3(ratings_df, args['S3_WRITE_PATH'])
    except Exception as e:
        print(f"Error running Glue Job: {e}")
