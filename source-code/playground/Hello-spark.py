from pyspark.sql import SparkSession
from utils.logger import Log4J

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Hello-Spark").master("local[3]").getOrCreate()
    logger = Log4J(spark)

    logger.warn("Hello Spark")

    data = [("sanu",26),("Jinu",20),("Renu",46)]

    spark_df = spark.createDataFrame(data).toDF("Name","Age")

    spark_df.show()

    spark.stop()