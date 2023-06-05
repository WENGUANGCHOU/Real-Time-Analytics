
## LOAD SPARK SESSION object
#YOUR CODE HERE
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType
import pyspark.sql.functions as f
###


SERVER = "broker:9092"

if __name__ == "__main__":
    ## create spark variable
    #YOUR CODE HERE
    spark = SparkSession.builder.getOrCreate()
    
    ## 
    spark.sparkContext.setLogLevel("ERROR")
    
    json_schema = StructType(
        [
            StructField("time", TimestampType()),
            StructField("id", StringType()),
            StructField("value", IntegerType()),
        ]
    )
    


    ## load stream data from topic streaming
    # topic subscription
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "streaming")
        .load()
    )
    
    # defining output as console with outputMode as append
    parsed = raw.select(
        "timestamp", f.from_json(raw.value.cast("string"), json_schema).alias("json")
    ).select(
        f.col("timestamp").alias("proc_time"),
        f.col("json").getField("time").alias("event_time"),
        f.col("json").getField("id").alias("id"),
        f.col("json").getField("value").alias("value"),
    )
    
    info = parsed.groupBy("id").count()
    
    
    query = (
        info.writeStream
        .outputMode("complete")
        .format("console")
        .start()
    )
    
    query.awaitTermination(60)
    query.stop()
    
