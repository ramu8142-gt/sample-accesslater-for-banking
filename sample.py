
import os

os.environ["SPARK_LOCAL_DIRS"] = r"E:\spark-temp"
os.environ["TEMP"] = r"E:\spark-temp"
os.environ["TMP"] = r"E:\spark-temp"
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data = [(1, "A"), (2, "B"), (3, "C")]
df = spark.createDataFrame(data, ["id", "name"])
df.show()

spark.stop()