import sys
from awsglue.transforms import *
from pyspark.sql.functions import regexp_replace, col
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("Starting job")


# --- Input/Output Paths ---
chunk_path = "s3://bank-transactions-pipeline/txn_chunks/chunk_00006.parquet"
importance_path = "s3://bank-transactions-pipeline/Customer_Importance.csv"
output_path = "s3://bank-transactions-pipeline/s33/"

# --- Load Data ---
df = spark.read.parquet(chunk_path)
importance_df = spark.read.option("header", True).csv(importance_path)
for colname in ["Source", "Target", "typeTrans"]:
    importance_df = importance_df.withColumn(colname, F.regexp_replace(F.col(colname), "'", ""))

# --- Cast and Clean Types ---
df = df.select(
    F.col("step").cast("int"),
    F.regexp_replace("customer", "'", "").alias("customer"),
    F.regexp_replace("age", "'", "").cast("int").alias("age"),
    F.regexp_replace("gender", "'", "").alias("gender"),
    F.regexp_replace("zipcodeOri", "'", "").alias("zipcodeOri"),
    F.regexp_replace("merchant", "'", "").alias("merchant"),
    F.regexp_replace("zipMerchant", "'", "").alias("zipMerchant"),
    F.regexp_replace("category", "'", "").alias("category"),
    F.col("amount").cast("double"),
    F.col("fraud").cast("int")
)
importance_threshold = 10000

df_joined = df.join(
    importance_df,
    (df.customer == importance_df.Source) & (df.merchant == importance_df.Target),
    how='left'
)

df_joined = df_joined.withColumn(
    "importance",
    F.when(F.col("Weight").cast("double") > importance_threshold, "HIGH").otherwise("LOW")
)

pat1 = df_joined.withColumn(
    "txn_count", 
    F.count("*").over(Window.partitionBy("customer", "step"))
).filter(F.col("txn_count") > 3) \
 .withColumn("pattern", F.lit("PatId1"))

pat2 = df_joined.filter(
    (F.col("importance") == "HIGH") & (F.col("amount") > 10000)
).withColumn("pattern", F.lit("PatId2"))

w3 = Window.partitionBy("zipcodeOri", "merchant").orderBy("step").rangeBetween(-4, 0)
pat3 = df_joined.withColumn(
    "zip_merchant_count", 
    F.count("*").over(w3)
).filter(F.col("zip_merchant_count") > 2) \
 .withColumn("pattern", F.lit("PatId3"))
pat1_sel = pat1.select("step", "customer", "amount", "pattern")
pat2_sel = pat2.select("step", "customer", "amount", "pattern")
pat3_sel = pat3.select("step", "customer", "amount", "pattern")

detections = pat1_sel.unionByName(pat2_sel).unionByName(pat3_sel).dropDuplicates()


# --- Batch in 50s and Write to S3 ---
detections = detections.withColumn("batch_id", F.floor(F.monotonically_increasing_id() / 50))
batch_ids = detections.select("batch_id").distinct().rdd.map(lambda r: r["batch_id"]).collect()

for batch_id in batch_ids:
    detections.filter(F.col("batch_id") == batch_id).drop("batch_id") \
        .write.mode("overwrite").option("header", True) \
        .csv(f"{output_path}batch_{batch_id}/")


job.commit()
