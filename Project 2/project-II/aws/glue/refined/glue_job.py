from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, when
from pyspark.sql.functions import count
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import DoubleType, LongType, StringType, IntegerType

conf = SparkConf()
conf.setAll(
    [('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem'),
     ('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.3'),
     ('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider'),
     ('spark.hadoop.fs.s3a.access.key', ''),
     ('spark.hadoop.fs.s3a.secret.key', ''),
     ('spark.hadoop.fs.s3a.session.token', ''),
     ])

spark = SparkSession \
    .builder \
    .appName('Glue_read_s3') \
    .config(conf=conf) \
    .getOrCreate()


def read_parquet_file(path: str):
    """
    Read a Parquet file from S3 and return a Spark dataframe.

    :param path: (str): The path to the Parquet file.
    :return: parquet_df: (Dataframe): The Spark dataframe.
    """
    try:
        parquet_df = spark.read.parquet(path)
        return parquet_df
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
        print(f"Successfully saved {df} file into s3")
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
        movies_df = df.withColumnRenamed("anolancamento", "release_year")
        movies_df = movies_df.withColumnRenamed("genero", "genre")
        movies_df = movies_df.withColumnRenamed("notamedia", "vote_average")
        movies_df = movies_df.withColumnRenamed("numerovotos", "vote_count")

        movies_df = movies_df.withColumn("release_year", col("release_year").cast(IntegerType()))
        movies_df = movies_df.withColumn("genre", col("genre"))
        movies_df = movies_df.withColumn("vote_average", col("vote_average").cast(DoubleType()))
        movies_df = movies_df.withColumn("vote_count", col("vote_count").cast(LongType()))

        return movies_df
    except Exception as e:
        print(f"Error preprocessing movies df: {e}")


def preprocess_top_rated_df(df):
    """
    Preprocess the genres df and  explode the genres' column.
    The genres column is a list of dictionaries, so we need to explode it to get the individual genres.
    The exploded genres column is a dictionary with the id and name of the genre, so we need to select the id and name columns.

    :param genres_df: (DataFrame): the genres dataframe.
    :return: genres_df: (DataFrame): the preprocessed genres dataframe.
    """
    try:
        top_rated_df = df.withColumnRenamed("movie_id", "id")
        top_rated_df = top_rated_df.withColumn("id", col("id").cast(StringType()))

        return top_rated_df
    except Exception as e:
        print(f"Error preprocessing genres df: {e}")
        return None


def preprocess_ratings_df(movies_df, top_rated_df):
    """
    Preprocess the ratings df by joining the movies and genres df on the genre_id column.
    Then, group the ratings df by movie_id and concatenate the genres into a single string separated by commas.

    :param movies_df: (DataFrame): the movies dataframe.
    :param genres_df: (DataFrame): the genres dataframe.
    :return: ratings_df: (DataFrame): the processed ratings dataframe.
    """
    try:
        movies_df = movies_df.select("id", "vote_average", "vote_count", "release_year", "genre")
        top_rated_df = top_rated_df.select("id", "vote_average", "vote_count", "release_year", "genre")

        ratings_df = top_rated_df.union(movies_df.select(*top_rated_df.columns)).drop("id").na.drop()
        ratings_df = generate_uuid(ratings_df, "rated_movie_genre_id")

        return ratings_df
    except Exception as e:
        print(f"Error preprocessing ratings df: {e}")
        return None


def generate_uuid(df, column_name: str):
    """
    Generate a unique ID for each row in the dataframe.

    :param df: (DataFrame): the input dataframe.
    :return: df: (DataFrame): the dataframe with a new column containing unique IDs.
    """
    try:
        df = df.withColumn(column_name, monotonically_increasing_id())
        return df
    except Exception as e:
        print(f"Error generating unique IDs: {e}")
        return None


def populate_dim_vote_average(df):
    """
    Populate the dim_vote_average table with the vote_average data.

    :param df: (DataFrame): the input DataFrame.
    :return: None
    """
    try:
        vote_average_df = df.select(col("vote_average")).groupby("vote_average").agg(count("*").alias("average")).drop(
            "average").na.drop()
        vote_average_df = generate_uuid(vote_average_df, "vote_average_id")

        # save_parquet_s3(vote_average_df, '')
        return vote_average_df
    except Exception as e:
        print(f"Error populating dim_vote_average table: {e}")


def populate_dim_vote_count(df):
    """
    Populate the dim_vote_count table with the vote_count data.

    :param df: (DataFrame): the input DataFrame.
    :return: None
    """
    try:
        vote_count_df = df.select(col("vote_count")).groupby("vote_count").agg(count("*").alias("count")).drop(
            "count").na.drop()
        vote_count_df = generate_uuid(vote_count_df, "vote_count_id")

        # save_parquet_s3(vote_count_df, '')
        return vote_count_df
    except Exception as e:
        print(f"Error populating dim_vote_count table: {e}")


def populate_dim_release_year(df):
    """
    Populate the dim_release_year table with the release_year data.

    :param df: (DataFrame): the input DataFrame.
    :return: None
    """
    try:
        release_year_df = df.select(col("release_year")).groupby("release_year").agg(count("*").alias("year")).drop(
            "year").na.drop()
        release_year_df = generate_uuid(release_year_df, "release_year_id")

        # save_parquet_s3(release_year_df, '')
        return release_year_df
    except Exception as e:
        print(f"Error populating dim_release_year table: {e}")


def populate_dim_genre(df):
    """
    Populate the dim_genre table with the genre data.

    :param df: (DataFrame): the input DataFrame.
    :return: None
    """
    try:
        genre_df = df.select(col("genre")) \
            .withColumn("genre", explode(split(col("genre"), ","))) \
            .withColumn("genre", when(col("genre") == "Science Fiction", "Sci-Fi").otherwise(col("genre"))) \
            .groupby("genre").agg(count("*").alias("count_genre")).drop("count_genre").na.drop()
        genre_df = generate_uuid(genre_df, "genre_id")

        # save_parquet_s3(genre_df, '')
        return genre_df
    except Exception as e:
        print(f"Error populating dim_genre table: {e}")
        return None


def populate_fact_rated_movies_genre(df, vote_average_df, vote_count_df, release_year_df, genre_df):
    """
    Populate the dim_rated_movies_genre table with the rated_movie_genre_id data.

    :param df: (DataFrame): the input DataFrame.
    :return: None
    """
    try:
        ratings_df = df.withColumn("genre", explode(split(col("genre"), ","))).withColumn("genre", when(
            col("genre") == "Science Fiction", "Sci-Fi").otherwise(col("genre")))

        rated_movies_genre_df = ratings_df \
            .join(vote_average_df, ratings_df.vote_average == vote_average_df.vote_average, "left") \
            .join(vote_count_df, ratings_df.vote_count == vote_count_df.vote_count, "left") \
            .join(release_year_df, ratings_df.release_year == release_year_df.release_year, "left") \
            .join(genre_df, ratings_df.genre == genre_df.genre, "left") \
            .select(col("rated_movie_genre_id"), col("vote_average_id"), col("vote_count_id"),
                    col("release_year_id"), col("genre_id"))

        del df, ratings_df, vote_average_df, vote_count_df, release_year_df, genre_df
        save_parquet_s3(rated_movies_genre_df, '')
    except Exception as e:
        print(f"Error populating dim_rated_movies_genre table: {e}")
        return None


if __name__ == '__main__':
    try:
        movies_df = read_parquet_file()
        top_rated_df = read_parquet_file()

        movies_df = preprocess_movies_df(movies_df)
        top_rated_df = preprocess_top_rated_df(top_rated_df)

        ratings_df = preprocess_ratings_df(movies_df, top_rated_df)
        del movies_df, top_rated_df

        vote_average_df = populate_dim_vote_average(ratings_df)
        vote_count_df = populate_dim_vote_count(ratings_df)
        release_year_df = populate_dim_release_year(ratings_df)
        genre_df = populate_dim_genre(ratings_df)

        populate_fact_rated_movies_genre(ratings_df, vote_average_df, vote_count_df, release_year_df, genre_df)
    except Exception as e:
        print(f"Error running Glue Job: {e}")
