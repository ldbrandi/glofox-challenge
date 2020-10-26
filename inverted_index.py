import shutil
import os
import pyspark.sql.functions as F

from utils import *


def listFiles(path):
    """
    Find all source files in the destination path.

    Parameters:
    path (str): Path for tje spurce files

    Returns:
    list: List of source files
    """
    files_list = []

    for name in os.listdir(path):
        file = os.path.join(path, name)

        if os.path.isfile(file):
            files_list.append(int(name))
    
    return files_list

def keyValueFile(rdd, file_num):
    """Only used for mapping the source file."""
    return rdd.map(lambda word: (word, file_num))

def addIdToFiles(spark, source_path):
    """
    Add the docId for each RDD, append to a list and
    Union all to create a single RDD.

    Returns:
    Union of every RDD in the source_path
    """
    list_of_rdd = []

    for file in listFiles(source_path):
        rdd = readFiles(spark.sparkContext, source_path + str(file))
        rdd = prepareRdd(rdd).distinct()
        rdd = keyValueFile(rdd, file)
        list_of_rdd.append(rdd)

    return spark.sparkContext.union(list_of_rdd)

def joinFilesDict(files_df, dictionary_df):
    """Left join files with dictionary and keep only wordId and docId columns"""
    pre_agg_df = files_df.join(
        dictionary_df, dictionary_df.word == files_df.word, how='left')

    return pre_agg_df.select(F.col('wordId'), F.col('docId'))

def main(spark, source_path, dict_path, output_path):
    """
    Call all functions needed for the process.

    Parameters:
    source_path (str): Source files like ./dataset/*
    dict_path (str): Dictionary path like ./output/dictonary
    output_path (str): Output for the inverted index like ./output/dict

    Returns:
    Create or replace an existing folder with the inverted index files
    """
    files_rdd = addIdToFiles(spark, source_path)
    files_df = spark.createDataFrame(files_rdd, ['word', 'docId'])
    dictionary_df = spark.read.parquet(dict_path) 
    full_df = joinFilesDict(files_df, dictionary_df)

    inverted_index_df = full_df \
        .groupBy(F.col('wordId')) \
        .agg(F.sort_array(F.collect_list(F.col('docId'))).alias('docId')) \
        .orderBy(F.col('wordId'))

    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    inverted_index_df.write.parquet(output_path)

    return inverted_index_df # for testing or integration purpouses


if __name__ == '__main__':
    source_path = './dataset/'
    dict_path = './output/dictionary'
    output_path = './output/inverted_index'

    main(createSparkSession(), source_path, dict_path, output_path)
