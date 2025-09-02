spark.conf.set("fs.s3a.access.key", "access")
spark.conf.set("fs.s3a.secret.key", "secret")
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

df = spark.read.format("json").load("bucketURI").cache()

#  Code to check that Keys work
# files = dbutils.fs.ls("bucketURI")
# for f in files:
#     print(f.name, f.size)

from pyspark.sql.functions import input_file_name, lit
df = (
  spark.readStream
       .format("cloudFiles")
       .option("cloudFiles.format", "text")
       .option("wholetext", "true")   
       .load("s3://an-calendly/data/")
       .withColumnRenamed("value", "raw_json")
       .withColumn("source_file", input_file_name())
)

df = df.withColumn("file_size_bytes", lit(None).cast("bigint")) \
       .withColumn("s3_last_modified", lit(None).cast("string"))

(
  df.writeStream
    .format("delta")
    .option("checkpointLocation", "s3://an-calendly/_autoloader/checkpoints/raw_webhooks")
    .trigger(processingTime="1 minute")
    .table("aliworkspace.calendly_bronze.raw_webhooks")
)



