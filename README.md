# glofox-challenge
Data engineering case.

## Setup used
Python 3.8.2 (3.6+ should be ok)  
Spark 3.0.1  

    pip install -r requirements.txt

## Outputs

    from utils import *

    spark = createSparkSession()
    dictionary_df = spark.read.parquet('./output/dictionary')
    inverted_index_df = spark.read.parquet('./output/inverted_index')

    dictionary_df.show()
    # or
    inverted_index_df.show()

## How to run

    python dictionary.py
    python inverted_index.py

The default spark configuration takes 4g and all local cores.  
To change the SparkConf, update the function createSparkSession() in utils.py

## Notes and improvements
- Containers were not used in this project
- Tests are not complete
- Make better use of functional programming