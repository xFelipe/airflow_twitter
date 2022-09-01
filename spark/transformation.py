import argparse
from os.path import join
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f

def get_tweets_data(df: DataFrame) -> DataFrame:
    tweets = df.select(
        f.explode('data').alias('tweets')
    )
    return tweets.select(
        'tweets.author_id',
        'tweets.conversation_id',
        'tweets.created_at',
        'tweets.id',
        'tweets.in_reply_to_user_id',
        'tweets.public_metrics.*',
        'tweets.text'
    )

def get_users_data(df: DataFrame) -> DataFrame:
    users = df.select(
        f.explode('includes.users').alias('users')
    )
    return users.select('users.*')

def export_json(df: DataFrame, dest: str):
    df.coalesce(1).write.mode('overwrite').json(dest)

def twitter_transform(spark, src, dest, process_date):
    df = spark.read.json(src)

    tweets_df = get_tweets_data(df)
    users_df = get_users_data(df)

    table_dest = join(dest, '{table_name}', f'process_date={process_date}')

    export_json(tweets_df, table_dest.format(table_name='tweet'))
    export_json(users_df, table_dest.format(table_name='user'))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Spark Twitter Transformation'
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    twitter_transform(spark, args.src, args.dest, args.process_date)
