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
import os
import sys
import configparser

from dependecies.spark import start_spark
from jobs.utils import read_data, calculate, write_data
from pyspark import SparkConf


def main():
    """Main script definition

    :return: None
    """

    conf = get_spark_app_config()
    app_name = conf.get("spark.app.name")
    master = conf.get("spark.master")
    # start spark application and get spark sessions, logger, and config
    spark, log, config = start_spark(
        app_name=app_name,
        master=master
    )

    if len(sys.argv) != 3:
        log.error("Usage: do_session.py <input_filename>")
        sys.exit(-1)

    log.warn('Starting Sessionization Job job')
    input_path = sys.argv[1]
    output_path = sys.argv[2] + '/'
    raw_data = read_data(spark, input_path)

    #
    final_data_size, final_data_cnt = calculate(spark, raw_data)

    # saving the data
    write_data(final_data_size, final_data_cnt, output_path)
    log.warn('Sessionization job is finished')
    spark.stop()


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


# Entry  point to the job
if __name__ == '__main__':
    main()
