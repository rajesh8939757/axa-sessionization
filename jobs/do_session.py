"""
do_session.py
Author: Rajesh Kumar
~~~~~~~~~~~~~

This python module is the entrypoint for the session
job. It can be submitted to spark cluster (or locally)
through spark-submit. For example as follows

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    do_session.py
"""

from dependecies.spark import start_spark
from pyspark.sql.functions import lag, col, concat, lit, unix_timestamp, when, sum
from pyspark.sql.window import Window


def main():
    """Main script definition

    :return: None
    """
    output_path = 'outdata/output/'
    # start spark application and get spark sessions, logger, and config
    spark, log, config = start_spark(
        app_name='job_sessionization'
    )

    log.warn('Sessionization job is up and running')
    raw_data = read_data(spark)

    #
    final_data_size, final_data_cnt = calculate(spark, raw_data)

    # saving the data
    write_data(final_data_size, final_data_cnt, output_path)
    log.warn('Sessionization job is finished')
    spark.stop()


def read_data(spark):
    """Load the csv data from local path.

    :param: spark: Spark Session object
    :return: Spark Dataframe
    """

    df = (
        spark
            .read
            .option('header', 'true')
            .option('inferSchema', 'true')
            .csv('./data/log20170201.csv'))

    return df


def calculate(spark, raw_data):
    """Calcluate the top 10 session with highest size downloaded files
    and highest number of files downloaded

    :param spark: SparkSession object
    :param raw_data: input data datafram of logs
    :return: Spark Dataframe, Spark Dataframe
    """
    # window specification for group by user and order by click time
    window_spec = Window.partitionBy('user_id').orderBy('session_tm')

    # select the required columns (date, time, cik, size)
    selected_data = raw_data \
        .withColumnRenamed('ip', 'user_id') \
        .withColumn('session_tm', concat(col('date'), lit(' '), col('time'))) \
        .withColumn('lag_tm', lag('session_tm').over(window_spec)) \
        .select('user_id', 'session_tm', 'lag_tm', 'cik', 'size')
    session_data = selected_data.withColumn('ts_diff', (unix_timestamp('session_tm') - unix_timestamp('lag_tm')) / 60) \
        .select('user_id', 'session_tm', 'lag_tm', 'ts_diff', 'cik', 'size')
    session_data.withColumn('ts_diff', when(col('ts_diff').isNull(), lit(0)).otherwise(col('ts_diff')))
    new_session_data = session_data.withColumn('is_new_session', when(col('ts_diff') > 30, 1).otherwise(0))
    user_session_id = new_session_data.withColumn('user_session_id', sum(col('is_new_session')).over(window_spec)) \
        .select('user_id', 'session_tm', 'lag_tm', 'user_session_id', 'cik', 'size')

    user_session_id.createOrReplaceTempView('session_data')

    user_sessions_by_size = """
        select user_session_id, sum(size) as cum_size
        from session_data
        group by user_session_id
        order by cum_size
        limit 10
    """
    user_sessions_by_size_df = spark.sql(user_sessions_by_size)
    user_sessions_by_size_df.show()

    user_sessions_by_count = """
            select user_session_id, count(cik) as cnt
            from session_data
            group by user_session_id
            order by cnt
            limit 10
        """
    user_sessions_by_count_df = spark.sql(user_sessions_by_count)
    user_sessions_by_count_df.show()

    return user_sessions_by_size_df, user_sessions_by_count_df


def write_data(final_data_size, final_data_cnt, output_path):
    (final_data_size
     .write
     .format('csv')
     .save(output_path + 'log_analytics_size.csv'))

    (final_data_cnt
     .write
     .format('csv')
     .save(output_path + 'log_analytics_cnt.csv'))


# Entry  point to the job
if __name__ == '__main__':
    main()
