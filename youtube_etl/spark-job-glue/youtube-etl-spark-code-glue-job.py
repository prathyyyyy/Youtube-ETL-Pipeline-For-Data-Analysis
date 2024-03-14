import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv,['Job_name'])

spark_context = SparkContext()
glueContext = GlueContext(spark_context)
job = Job(glueContext)
job.init(args["JOB_NAME"],args)

predicate_pushdown = "region in ('in','ca','us')"

data_source = (glueContext.create_dynamic_frame.from_catalog(
    database = "db_youtube_raw",
    table_name = "raw_statistics",
    transformation_ctx = "datasource0"
))

apply_mapping = ApplyMapping.apply()