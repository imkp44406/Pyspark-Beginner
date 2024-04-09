from pyspark.sql import SparkSession,functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import col,expr


if __name__ == "__main__":

    spark = SparkSession.builder.master("local[3]").appName("Avrage-No-Of-Friends").getOrCreate()

    schema = StructType([\
        StructField('id',IntegerType(),True),\
        StructField('name',StringType(),True),\
        StructField('age',IntegerType(),True),\
        StructField('friends',IntegerType(),True)])

    input_df = spark.read.format('csv').schema(schema)\
    .option('header','false').option('mode','permissive')\
    .option('path',"file:///C:/Pycharm/Pyspark-Beginner/data/fakefriends.csv").load()

    input_df.select('age','friends').groupBy(col('age')).agg(func.round(func.avg(col('friends')),2).alias('avg_friends'))\
        .orderBy(col('avg_friends'),ascending=False).show(10,False)

    spark.stop()