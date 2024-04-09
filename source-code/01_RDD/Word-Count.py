from pyspark import SparkConf,SparkContext
import re

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("Word-Count")
    sc = SparkContext(conf=conf)

    book = sc.textFile("file///C:/Pycharm/Pyspark-Beginner/data/Book.txt")
    # words = book.flatMap(lambda x: x.split())
    words = book.flatMap(lambda x: re.compile(r'\W+', re.UNICODE).split(x.lower()))
    row = words.map(lambda x:(x,1))
    word_count = row.reduceByKey(lambda x,y: x+y)
    sorted_rdd = word_count.sortBy(lambda x: x[1],ascending=False)
    # result = sorted_rdd.collect()
    # for key,val in result:
    #     print(f"""Word-> {key.encode("ascii","ignore").decode()} Count-> {val}""")
    result = sorted_rdd.take(10)
    for key,val in result:
        print(f"""Word-> {key.encode("ascii","ignore").decode()} Count-> {val}""")
    sc.stop()