from pyspark import SparkConf
from pyspark.sql import SparkSession,functions as func
from pyspark.sql.functions import col

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("Most-Popular-Superhero")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    marvel_graph_df = spark.read.format("text").option("header","false")\
        .option("mode","permissive").option("path","file:///C:/Pycharm/Pyspark-Beginner/data\Marvel-Graph.txt").load()

    popular_hero_by_id = marvel_graph_df.withColumn("ids",func.split(col('value')," "))\
        .withColumn("superhero_id",col("ids")[0].cast("int"))\
        .withColumn("count_conn",func.size(col("ids"))-1)\
        .drop(col("value")).drop(col("ids"))\
        .groupBy(col("superhero_id")).agg(func.sum(col("count_conn")).alias("total_conn"))

    marvel_names_schema = """id int,name string"""

    marvel_names_df = spark.read.format("csv").schema(marvel_names_schema)\
        .option("sep"," ").option("mode","permissive")\
        .option("header","false").option("path","file:///C:/Pycharm/Pyspark-Beginner/data/Marvel-Names.txt")\
        .load()

    marvel_names_df_bdsct = func.broadcast(marvel_names_df)

    result_df = popular_hero_by_id.alias("a").join(marvel_names_df_bdsct.alias("b"),\
                                                   popular_hero_by_id["superhero_id"] == marvel_names_df_bdsct["id"],"left")\
        .select("a.superhero_id","b.name","a.total_conn").orderBy(col("total_conn"),ascending=False)

    result_df.show(1,False)

    spark.stop()