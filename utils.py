from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def createSparkSession():
    """Create and return the SparkContext(sc)."""
    conf = SparkConf() \
        .setMaster('local[*]') \
        .setExecutorEnv('spark.executor.memory', '4g') \
        .setExecutorEnv('spark.driver.memory', '4g') \
        .setAppName('glofox') \
    
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    return spark

def readFiles(sc, path):
    """Read file(s) and transform into RDD."""
    return sc.textFile(path)

def cleanText(text):
    """
    Remove special characters and set lower case.

    Parameters:
    text (str): Text to be cleaned and lower cased

    Returns:
    list: Clean text
    """
    trash = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~-'
    lowercased_text = text.lower()

    for char in trash:
        lowercased_text = lowercased_text.replace(char, '')

    return lowercased_text

def prepareRdd(rdd):
    """Remove invalid characters, split in words and remove blank spaces."""
    return rdd.map(cleanText) \
            .flatMap(lambda text: text.split(' ')) \
            .filter(lambda text: text != '')
