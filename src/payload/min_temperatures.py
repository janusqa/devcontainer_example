from pyspark.sql import SparkSession

# from pyspark import SparkConf, SparkContext


def min_temperatures_year_1800(spark: SparkSession, data_dir: str):
    # conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
    # sc = SparkContext(conf=conf)

    sc = spark.sparkContext

    def parse_line(line):
        fields = line.split(",")
        stationID = fields[0]
        entryType = fields[2]
        temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
        return (stationID, entryType, temperature)

    lines = sc.textFile(f"file://{data_dir}/1800.csv")
    parsed_lines = lines.map(parse_line)
    min_temps = parsed_lines.filter(lambda x: "TMIN" == x[1])
    max_temps = parsed_lines.filter(lambda x: "TMAX" == x[1])
    station_temps_min = min_temps.map(lambda x: (x[0], x[2]))
    station_temps_max = max_temps.map(lambda x: (x[0], x[2]))
    min_temps = station_temps_min.reduceByKey(lambda x, y: min(x, y))
    max_temps = station_temps_max.reduceByKey(lambda x, y: max(x, y))
    results_min = min_temps.collect()
    results_max = max_temps.collect()

    print("-" * 50)
    print("Min Temps in Year 1800")
    print("-" * 50)
    for result in results_min:
        print(f"{result[0]}\t{result[1]:.2f}F")
    print()
    print("-" * 50)
    print("Max Temps in Year 1800")
    print("-" * 50)
    for result in results_max:
        print(f"{result[0]}\t{result[1]:.2f}F")
