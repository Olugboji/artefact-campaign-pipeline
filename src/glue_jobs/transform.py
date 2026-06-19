import sys, os
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType

args   = getResolvedOptions(sys.argv, ['ENV','RAW_BUCKET','STAGING_BUCKET'])
ENV    = args['ENV']
RAW    = f"s3://{args['RAW_BUCKET']}"
STAGE  = f"s3://{args['STAGING_BUCKET']}"

sc         = SparkContext()
glueCtx    = GlueContext(sc)
spark      = glueCtx.spark_session

events = spark.read.option('header', True).csv(f'{RAW}/events/')
spend  = spark.read.option('header', True).csv(f'{RAW}/spend/')

# De-duplicate event_id
events = events.dropDuplicates()
w = Window.partitionBy('event_id').orderBy(
    F.to_timestamp('event_ts').asc_nulls_last())
events = (events.withColumn('_rn', F.row_number().over(w))
                .filter(F.col('_rn') == 1).drop('_rn'))

# Parse timestamps - 5 known formats
fmts = ["yyyy-MM-dd'T'HH:mm:ss'Z'", "dd/MM/yyyy'T'HH:mm:ss'Z'",
        "yyyy/MM/dd'T'HH:mm:ss'Z'", "MM-dd-yyyy'T'HH:mm:ss'Z'",
        "yyyy-MM-dd HH:mm:ss"]
parsed = F.lit(None).cast('timestamp')
for fmt in fmts:
    parsed = F.coalesce(parsed, F.to_timestamp('event_ts', fmt))
events = (events.withColumn('event_ts_utc', parsed)
                .withColumn('dq_flag_invalid_ts', F.col('event_ts_utc').isNull()))

events_clean = events.filter(~F.col('dq_flag_invalid_ts'))
quarantine   = events.filter( F.col('dq_flag_invalid_ts'))

# Strip tracking params from page_url
@F.udf(StringType())
def clean_path(url):
    if url is None: return None
    from urllib.parse import urlsplit
    return urlsplit(url).path
events_clean = events_clean.withColumn('page_path', clean_path('page_url'))

# Normalize spend dates
spend = spend.withColumn('date_parsed',
    F.when(F.col('date').rlike(r'^\d{4}-\d{2}-\d{2}$'),
           F.to_date('date', 'yyyy-MM-dd'))
    .otherwise(F.to_date('date', 'dd/MM/yyyy'))
)
spend = (spend.withColumn('dq_flag_null_spend',     F.col('spend_usd').isNull())
              .withColumn('dq_flag_negative_spend', F.col('spend_usd').cast('double') < 0))

# Write outputs
events_clean.write.mode('overwrite').parquet(f'{STAGE}/events_clean/')
quarantine.write.mode('overwrite').parquet(f'{STAGE}/quarantine/')
spend.write.mode('overwrite').parquet(f'{STAGE}/spend_clean/')
print(f'[{ENV}] Transform complete: {events_clean.count()} clean rows, {quarantine.count()} quarantined.')