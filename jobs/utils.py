"""
utils.py
Author: Rajesh Kumar
~~~~~~~~~~~~~~~~~~~~

This python module is the utilies for
the main functions.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def read_data(spark, datafile):
    """Load the csv data from local path.

    :param: spark: Spark Session object
    :param: str: Data file location
    :return: Spark Dataframe
    """

    df = (
        spark
            .read
            .option('header', 'true')
            .option('inferSchema', 'true')
            .csv(datafile))

    return df


def calculate(spark, raw_data):
    """Calculate the top 10 session with highest size downloaded files
    and highest number of files downloaded

    :param spark: SparkSession object
    :param raw_data: input data dataframe
    :return: Spark Dataframe, Spark Dataframe
    """

    # window specification for group by user and order by click time
    window_spec = Window.partitionBy('user_id').orderBy('session_tm')

    # select the required columns (date, time, cik, size)
    selected_data = raw_data \
        .withColumnRenamed('ip', 'user_id') \
        .select('user_id', 'date', 'time', 'size')

    session_data = selected_data \
        .withColumn('session_tm', F.to_timestamp(F.concat(F.col('date'), F.lit(' '), F.col('time'))))

    lag_data = session_data \
        .withColumn('lag_tm', F.lag('session_tm').over(window_spec)) \
        .select('user_id', 'session_tm', 'lag_tm', 'size')

    ts_diff_cal = lag_data \
        .withColumn('ts_diff',((F.unix_timestamp('session_tm') - F.unix_timestamp('lag_tm')) / 60)).na.fill(0)

    # spark 3.0.0 fixed for bug
    ts_diff_array = (
        ts_diff_cal
            .withColumn('flag_value', F.collect_list(F.col('ts_diff'))
                        .over(window_spec)
                        .cast('array<int>')))

    reduce_expr = "aggregate(flag_value, 0, (acc, ele) -> IF(acc <= 30, acc + ele, ele))"
    sessions = ts_diff_array\
        .withColumn('session', F.expr(reduce_expr))

    sessions = sessions \
        .withColumn('session_new', F.when(F.col('session') > 30, 1).otherwise(0))

    final_sessions = sessions \
        .withColumn('session_id', F.concat(F.col('user_id'), F.lit('--S'), F.sum(F.col('session_new')).over(window_spec))) \
        .select('user_id', 'session_tm', 'size', 'session_id')

    final_sessions.createOrReplaceTempView('session_data')

    user_sessions_by_size = """
        select session_id, sum(size) as cum_size
        from session_data
        group by session_id
        order by cum_size
        limit 10
    """
    user_sessions_by_size_df = spark.sql(user_sessions_by_size)
    user_sessions_by_size_df.show()

    user_sessions_by_count = """
            select session_id, count(*) as cnt
            from session_data
            group by session_id
            order by cnt
            limit 10
        """
    user_sessions_by_count_df = spark.sql(user_sessions_by_count)
    user_sessions_by_count_df.show()

    return user_sessions_by_size_df, user_sessions_by_count_df


def write_data(final_data_size, final_data_cnt, output_path):
    """Write the data to local file
    in csv format

    :param final_data_size: Spark Dataframe
    :param final_data_cnt: Spark Dataframe
    :param output_path: Output path location
    """

    (final_data_size
     .write
     .mode('overwrite')
     .format('csv')
     .save(output_path + 'log_analytics_size'))

    (final_data_cnt
     .write
     .mode('overwrite')
     .format('csv')
     .save(output_path + 'log_analytics_cnt'))
