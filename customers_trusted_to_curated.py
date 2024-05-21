import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1716320329077 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-trusted-zone/customer/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1716320329077")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1716320367024 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-trusted-zone/accelerometer/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1716320367024")

# Script generated for node Join
Join_node1716320530791 = Join.apply(frame1=CustomerTrusted_node1716320329077, frame2=AccelerometerTrusted_node1716320367024, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1716320530791")

# Script generated for node FilterForAccelerometerDataOnly
FilterForAccelerometerDataOnly_node1716321101437 = Filter.apply(frame=Join_node1716320530791, f=lambda row: (not(row["z"] == 0) or not(row["y"] == 0) or not(row["z"] == 0)), transformation_ctx="FilterForAccelerometerDataOnly_node1716321101437")

# Script generated for node Drop Fields
DropFields_node1716320611454 = DropFields.apply(frame=FilterForAccelerometerDataOnly_node1716321101437, paths=["z", "y", "x", "user", "timestamp"], transformation_ctx="DropFields_node1716320611454")

# Script generated for node Customer Curated
CustomerCurated_node1716320751627 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1716320611454, connection_type="s3", format="json", connection_options={"path": "s3://stedi-curated-zone/", "partitionKeys": []}, transformation_ctx="CustomerCurated_node1716320751627")

job.commit()
