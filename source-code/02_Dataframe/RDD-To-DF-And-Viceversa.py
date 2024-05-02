from pyspark.sql import SparkSession,Row
from pyspark import SparkConf
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
def parseLine(s):
    fields = s.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

if __name__ == '__main__':

    conf = SparkConf().setMaster('local[3]')\
        .setAppName('RDD-To-DF-And-Viceversa')\
        .set("spark.executor.cores", "2")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    sc = spark.sparkContext

    schema = StructType([\
        StructField('id',IntegerType(),True),\
        StructField('name',StringType(),True),\
        StructField('age',IntegerType(),True),\
        StructField('friends',IntegerType(),True)])

    input_rdd = sc.textFile('file:///C:/Pycharm/Pyspark-Beginner/data/fakefriends.csv')

    formated_rdd = input_rdd.map(parseLine)

    rdd_to_df_1 = spark.createDataFrame(formated_rdd)
    rdd_to_df_1.printSchema()
    rdd_to_df_2 = spark.createDataFrame(formated_rdd,schema=schema)
    rdd_to_df_2.printSchema()
    rdd_to_df_3 = formated_rdd.toDF(schema=schema)
    rdd_to_df_3.printSchema()

    df_to_rdd = rdd_to_df_3.rdd
    res = df_to_rdd.take(5)
    print(res)
    for ele in res:
        print(tuple(ele))

    spark.stop()