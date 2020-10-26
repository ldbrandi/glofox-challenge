import shutil
import os

from utils import *
import pyspark.sql.functions as F


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
    return rdd.map(lambda word: (word, file_num))

def addIdToFiles(spark, source_path):
    list_of_rdd = []

    for file in listFiles(source_path):
        rdd = readFiles(spark.sparkContext, source_path + str(file))
        rdd = prepareRdd(rdd).distinct()
        rdd = keyValueFile(rdd, file)
        list_of_rdd.append(rdd)

    return spark.sparkContext.union(list_of_rdd)

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

    pre_agg_df = files_df.join(
        dictionary_df, dictionary_df.word == files_df.word, how='left')

    filtered_df = pre_agg_df.select(F.col('wordId'), F.col('docId'))

    inverted_index_df = filtered_df.groupBy(F.col('wordId')) \
        .agg(F.sort_array(F.collect_list(F.col('docId'))).alias('docId')) \
        .orderBy(F.col('wordId'))

    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    inverted_index_df.write.parquet(output_path)

    return inverted_index_df


if __name__ == '__main__':
    source_path = './dataset/'
    dict_path = './output/dictionary'
    output_path = './output/inverted_index'

    main(createSparkSession(), source_path, dict_path, output_path)

# source_path = './dataset/'
# dict_path = './output/dictionary'
# output_path = './output/inverted_index'

# df = main(createSparkSession(), source_path, dict_path, output_path)

# import os
# from utils import *
# lista = listFiles('./dataset/')  
# spark = createSparkSession() 
# rdd0 = readFiles(spark.sparkContext, './dataset/' + str(lista[0]))  
# rdd0 = prepareRdd(rdd0).distinct()
# rdd0 = rdd0.map(lambda word:(word,0))
# dict_df = spark.read.parquet('./output/dictionary') 

# list_of_rdd = []
# for file in lista:
#     rdd = readFiles(spark.sparkContext, './dataset/' + str(file))
#     rdd = prepareRdd(rdd).distinct()
#     rdd = keyValueFile(rdd, file)
#     list_of_rdd.append(rdd)
# final_rdd = spark.sparkContext.union(list_of_rdd)

# df = spark.createDataFrame(rdd, ['word', 'file'])

# files_df = spark.createDataFrame(final_rdd, ['word', 'file'])

# pre_agg_df = files_df.join(
#     dict_df, dict_df.word == files_df.word, how='left')


# df.groupBy(F.col('wordId')).agg(F.sort_array(F.collect_list(F.col('docId'))).alias('docId')).orderBy(F.col('wordId')).show()