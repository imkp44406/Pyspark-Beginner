from pyspark import SparkConf,SparkContext
import collections

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("Rating-Histogram")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("file:///C:/Pycharm/Pyspark-Beginner/data/u.data")
    ratings = lines.map(lambda x: x.split()[2])
    result = ratings.countByValue()
    sortedResults = collections.OrderedDict(sorted(result.items()))
    for key, value in sortedResults.items():
        print("%s %i" % (key, value))
    sc.stop()