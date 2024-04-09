from pyspark import SparkConf,SparkContext

def parseLine(s):
    ele = s.split(",")
    c_id = int(ele[0])
    amt = float(ele[2])
    return (c_id,amt)

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[3]").setAppName("Total-Amount-Spent")
    sc = SparkContext(conf=conf)
    input = sc.textFile("file///C:/Pycharm/Pyspark-Beginner/data/customer-orders.csv")
    fmt_data = input.map(parseLine)
    total_spent = fmt_data.reduceByKey(lambda x,y: round(x+y,2))
    sorted_by_max_spent = total_spent.sortBy(lambda x:x[1],ascending=False)
    res = sorted_by_max_spent.collect()
    for key,val in res:
        print(f"Customer ID-> {key} Amount-> {val}")
    sc.stop()

