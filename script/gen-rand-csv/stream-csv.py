from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()



s = spark.readStream.option("delimiter", ",").schema("id INT, val1 INT, val2 INT").csv('file:/root/dump/source1/')
s.show()
s.writeStream.format("console").option("truncate","false").start()


df = spark.read.option("delimiter", ",").option("header","true").csv('file:/root/dump/source1/', inferSchema=True)