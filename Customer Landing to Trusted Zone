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

# Script generated for node Amazon S3
AmazonS3_node1716317821014 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-landing-zone/customer/landing/customer-1691348231425.json"]}, transformation_ctx="AmazonS3_node1716317821014")

# Script generated for node PrivacyFilter
PrivacyFilter_node1716314720220 = Filter.apply(frame=AmazonS3_node1716317821014, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="PrivacyFilter_node1716314720220")

# Script generated for node Amazon S3
AmazonS3_node1716314770654 = glueContext.getSink(path="s3://stedi-trusted-zone/customer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1716314770654")
AmazonS3_node1716314770654.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1716314770654.setFormat("json")
AmazonS3_node1716314770654.writeFrame(PrivacyFilter_node1716314720220)
job.commit()
