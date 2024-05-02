from pyspark import SparkConf
from pyspark.sql import SparkSession,functions as func
from pyspark.sql.functions import col
import codecs

def loadMovieName():
    movie_dict={}
    with codecs.open("C:/Pycharm/Pyspark-Beginner/data/u.item","r",encoding='ISO-8859-1', errors='ignore') as file:
        for line in file:
            fields = line.split("|")
            movie_dict[int(fields[0])] = fields[1]
    return movie_dict

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("Most-Popular-Movie-2")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    u_data_schema = """user_id int,movie_id int,ratings int,epoc_ts bigint"""
    u_data_df = spark.read.format('csv').schema(u_data_schema)\
        .option("header","false").option("sep","\t")\
        .option("mode","permissive").option("path","file:///C:/Pycharm/Pyspark-Beginner/data/u.data").load()
    most_populat_movies = u_data_df.select("movie_id","user_id").groupBy(col("movie_id")).agg(func.count(col("user_id")).alias("watch_count"))

    u_item_dict = spark.sparkContext.broadcast(loadMovieName())
    def returnMovieName(movieId):
        return u_item_dict.value[movieId]
    movieNameUDF = func.udf(returnMovieName)

    result_df = most_populat_movies.withColumn("movie_name",movieNameUDF(col("movie_id")))

    result_df.show()
    spark.stop()