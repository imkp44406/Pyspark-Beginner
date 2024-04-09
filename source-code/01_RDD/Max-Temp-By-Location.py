from pyspark import SparkConf,SparkContext

def parseLine(s):
    ele= s.split(",")
    id= ele[0]
    year= int(ele[1][0:4])
    entrytype= ele[2]
    temp= round((float(ele[3]) * 0.1 * (9/5))+32,2)
    return (id,year,entrytype,temp)

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("Max-Temp-By-Location")
    sc=SparkContext(conf=conf)

    input = sc.textFile("file///C:/Pycharm/Pyspark-Beginner/data/1800.csv")
    rows = input.map(parseLine)
    filtered_data = rows.filter(lambda x: (x[1] == 1800 and x[2].upper() =='TMAX') )
    station_temps = filtered_data.map(lambda x: (x[0],x[3]))
    min_temps = station_temps.reduceByKey(lambda x,y: max(x,y))

    results = min_temps.collect()
    for key,val in results:
        print(f"Station ID-> {key} Temp-> {val}F")
    sc.stop()