"""
spark.py
Author: Rajesh Kumar
~~~~~~~~~~~~~~~~~~~~

This module is helper function to start
Spark session
"""

import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependecies import logging


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    """Start Spark session, get Spark logger and load config files.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    # detect execution environment
    flag_rep = not (hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_rep or flag_debug):
        # get Spark Session factory
        spark = SparkSession \
                .builder \
                .appName(app_name)
    else:
        # get Spark Session factory
        spark = SparkSession \
                .builder \
                .master(master) \
                .appName(app_name)

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark.config('spark.files', spark_files)

        for key, val in spark_config.items():
            spark.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark.getOrCreate()
    spark_logger = logging.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict
