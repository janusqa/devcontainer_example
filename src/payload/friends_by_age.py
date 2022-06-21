from pyspark.sql import SparkSession

# from pyspark import SparkConf, SparkContext


def friends_by_age(spark: SparkSession, data_dir: str):
    # conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
    # sc = SparkContext(conf = conf)

    # Any functions used by map should be place in the same context
    # in which map is called. Like parse_line below
    def parse_line(line):
        fields = line.split(",")
        age = int(fields[2])
        numFriends = int(fields[3])
        return (age, numFriends)

    sc = spark.sparkContext
    lines = sc.textFile(f"file://{data_dir}/fakefriends.csv")
    rdd = lines.map(parse_line)
    totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1])
    )
    averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
    results = averagesByAge.collect()
    for result in results:
        print(result)
