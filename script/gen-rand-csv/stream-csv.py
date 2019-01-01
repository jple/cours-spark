from pyspark.sql import SparkSession

import pyspark.sql.functions as F


spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


# afficher les dataframe du stream
s1 = spark.readStream.option("sep", ",").schema("id INT, val1 INT, val2 INT").csv('file:/root/dump/source1/', header=True)
s1.writeStream.format("console").option("truncate","false").start()


# combiner deux streams
s1 = spark.readStream.option("sep", ",").schema("id INT, val1 INT, val2 INT").csv('file:/root/dump/source1/', header=True)
s2 = spark.readStream.option("sep", ",").schema("id INT, val1 INT, val2 INT").csv('file:/root/dump/source2/', header=True)

s1_wm = s1.withWatermark("s1_time", "3 seconds")
s2_wm = s2.withWatermark("s2_time", "3 seconds")

mean_val1_s1 = s1_wm.select(F.avg('val1')).withColumn('source', F.lit(1))
mean_val1_s2 = s2_wm.select(F.avg('val1')).withColumn('source', F.lit(2))

mean_val1 = mean_val1_s1.union(mean_val1_s2)
mean_val1_s1.writeStream.format("console").option("truncate","false").start()





mean_val1.writeStream.format("console").option("truncate","false").start()


df = spark.read.option("delimiter", ",").option("header","true").csv('file:/root/dump/source1/', inferSchema=True)
df.select(F.avg('val1')).withColumn('source', F.lit(1)).show()