import pytest
from dictionary import *

sc = createSparkContext()

def test_read_files():
    import pyspark
    assert isinstance(readFiles(sc, './dataset/0'), pyspark.rdd.RDD)

def test_prepare_rdd():
    input = sc.parallelize(['$! cat #dog', '', 'dog'])

    expected = ['cat', 'dog', 'dog']
    got = input.map(cleanText) \
            .flatMap(lambda text: text.split(' ')) \
            .filter(lambda text: text != '')
    assert expected == got