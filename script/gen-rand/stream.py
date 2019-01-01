from pyspark.sql import SparkConf, SparkSession
#from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext

# Spark init 
#conf = SparkConf().setAppName("Streaming Bob")#.set("spark.speculation","true")
spark = SparkSession.builder.master("yarn").appName("red_qaqc").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
#sqlContext = SQLContext(sc)

sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 10)
inputDS = ssc.textFileStream('dump/')
inputDS.pprint()
ssc.start()
#ssc.awaitTermination()
