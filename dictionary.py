import shutil
from os import path

from utils import *


def makeDictionary(rdd):
    """Create a tuple (key,value) to be used as dictionary."""
    return rdd.distinct() \
            .zipWithIndex()

def main(spark, input_path, output_path):
    """
    Call all functions needed for the process.

    Parameters:
    input_path (str): Source files like ./dataset/*
    output_path (str): Output for the dictionary like ./output/dict

    Returns:
    Create or replace an existing directory with the dictonary files
    """
    raw_rdd = readFiles(spark.sparkContext, input_path)
    prepared_rdd = prepareRdd(raw_rdd)
    dictionary_rdd = makeDictionary(prepared_rdd)

    if path.exists(output_path):
        shutil.rmtree(output_path)

    df = spark.createDataFrame(dictionary_rdd, ["word", "wordId"])
    df.repartition(10).write.parquet(output_path)

    return df # testing or integration purpouses

    
if __name__ == '__main__':
    input_path = './dataset/*'
    output_path = './output/dictionary'
    
    main(createSparkSession(), input_path, output_path)
