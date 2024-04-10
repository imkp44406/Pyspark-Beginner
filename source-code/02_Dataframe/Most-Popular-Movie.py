from pyspark import SparkConf
from pyspark.sql import SparkSession,functions as func
from pyspark.sql.functions import col,expr,broadcast
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("Most-Popular-Movie")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    u_data_schema = """user_id int,movie_id int,ratings int,epoc_ts bigint"""
    u_data_df = spark.read.format('csv').schema(u_data_schema)\
        .option("header","false").option("sep","\t")\
        .option("mode","permissive").option("path","file:///C:/Pycharm/Pyspark-Beginner/data/u.data").load()
    most_populat_movies = u_data_df.select("movie_id","user_id").groupBy(col("movie_id")).agg(func.count(col("user_id")).alias("watch_count"))

    u_item_schema = StructType([\
        StructField("movie_id",IntegerType(),True),\
        StructField("movie_name",StringType(),True)])

    u_item_df = spark.read.format("csv").schema(u_item_schema).option("sep","|")\
        .option("header","false").option("mode","permissive")\
        .option("path","file:///C:/Pycharm/Pyspark-Beginner/data/u.item").load()

    u_item_df_bcst=broadcast(u_item_df)

    result_df = most_populat_movies.alias("mid").join(u_item_df_bcst.alias("mnm"),(most_populat_movies["movie_id"] == u_item_df_bcst["movie_id"]),"left")\
        .select("mid.movie_id","mnm.movie_name","mid.watch_count").orderBy(col("watch_count"),ascending=False)

    result_df.show(10)

    spark.stop()