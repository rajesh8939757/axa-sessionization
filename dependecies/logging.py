"""
logging.py
Author: Rajesh Kumar
~~~~~~~~~~~~~~~~~~~~
This module enabling Log4j logging for PySpark using.
"""


class Log4j(object):
    """Wrapper class for Log4j JVM object.

    :param spark: SparkSession object
    """

    def __init__(self, spark):
        # get spark app details
        root_class = "com.axa.engineering.spark.challenge"
        conf = spark.sparkContext.getConf()
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    def error(self, message):
        """Log an error.

        :param: Error message to write to log
        :return: None
        """

        self.logger.warn(message)
        return None

    def warn(self, message):
        """Log a Warning.

        :param: Warning message to write to Log
        :return: None
        """

        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.

        :param: Information message to write to log
        :return: None
        """

        self.logger.info(message)
        return None

    def debug(self, message):
        """Log a Debug information.

        :param: Debug message to write to log
        :return: None
        """

        self.logger.debug(message)
        return None
