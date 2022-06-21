from pyspark.sql import SparkSession


def customer_orders(spark: SparkSession, data_dir: str):
    def parse_line(line):
        fields = line.split(",")
        customer_id = int(fields[0])
        amount = float(fields[2])
        return (customer_id, amount)

    sc = spark.sparkContext

    input = sc.textFile(f"file://{data_dir}/customer-orders.csv")

    spend_by_customer = (
        input.map(parse_line)
        .reduceByKey(lambda x, y: x + y)
        .sortBy(lambda x: x[1], False)
    )

    results = spend_by_customer.collect()

    for result in results:
        print(f"CustomerID: {result[0]}\tSpend: {result[1]:.2f}")
