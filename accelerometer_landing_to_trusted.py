import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node S3 Accelerometer Data Landed
S3AccelerometerDataLanded_node1716318912905 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-landing-zone/accelerometer/landing/"], "recurse": True}, transformation_ctx="S3AccelerometerDataLanded_node1716318912905")

# Script generated for node S3 Customer Data Trusted
S3CustomerDataTrusted_node1716318997085 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-trusted-zone/customer/"], "recurse": True}, transformation_ctx="S3CustomerDataTrusted_node1716318997085")

# Script generated for node Join
Join_node1716319061139 = Join.apply(frame1=S3AccelerometerDataLanded_node1716318912905, frame2=S3CustomerDataTrusted_node1716318997085, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1716319061139")

# Script generated for node Drop Fields
DropFields_node1716319377409 = DropFields.apply(frame=Join_node1716319061139, paths=["email", "phone"], transformation_ctx="DropFields_node1716319377409")

# Script generated for node S3 Accelerometer Data Trusted
S3AccelerometerDataTrusted_node1716319496655 = glueContext.getSink(path="s3://stedi-trusted-zone/accelerometer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="S3AccelerometerDataTrusted_node1716319496655")
S3AccelerometerDataTrusted_node1716319496655.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
S3AccelerometerDataTrusted_node1716319496655.setFormat("json")
S3AccelerometerDataTrusted_node1716319496655.writeFrame(DropFields_node1716319377409)
job.commit()
