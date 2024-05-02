from pyspark.sql import SparkSession,functions as func
from pyspark.sql.functions import col,expr
from pyspark import SparkConf

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[3]').setAppName('Word-Count').set('spark.executor.cores','2')

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    input_df = spark.read.format('text').option('path','file:///C:/Pycharm/Pyspark-Beginner/data/Book.txt').load()

    res = input_df.select(func.explode(func.split(func.lower(col('value')),'\\W+')).alias('words')).filter(expr("words <> ''"))\
        .groupBy(col('words')).agg(func.count(col('*')).alias('count')).orderBy(col('count'),ascending=False)

    res.show(10,False)

    spark.stop()