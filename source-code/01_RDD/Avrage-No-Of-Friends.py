from pyspark import SparkConf, SparkContext


def parseLine(s):
    ele = s.split(",")
    age = int(ele[2])
    count = int(ele[3])
    return (age, count)


if __name__ == "__main__":

    conf = SparkConf().setMaster("local[3]").setAppName("Avg-No-Of-Friends")
    sc = SparkContext(conf=conf)

    fake_friends = sc.textFile("file///C:/Pycharm/Pyspark-Beginner/data/fakefriends.csv")
    lines = fake_friends.map(parseLine)
    # count_age = lines.map(lambda x:(x[0],(x[1],1))) // THIS WILL KATE BOTH KEY & VALUES
    count_age = lines.mapValues(lambda x: (x,1))
    total_by_age = count_age.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
    result = total_by_age.mapValues(lambda x: round(x[0]/x[1],2))
    sorted_rdd = result.sortBy(lambda x: x[1],ascending=False)

    sorted_data = sorted_rdd.collect()
    for key,val in sorted_data:
        print(f"age-> {key} Avg Friends-> {val}")
    sc.stop()


