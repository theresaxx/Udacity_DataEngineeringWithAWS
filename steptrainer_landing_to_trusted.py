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

# Script generated for node S3 Step trainer landing
S3Steptrainerlanding_node1716384167237 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-landing-zone/step_trainer/landing"], "recurse": True}, transformation_ctx="S3Steptrainerlanding_node1716384167237")

# Script generated for node S3 Customer curated
S3Customercurated_node1716384247005 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-curated-zone/customer/"], "recurse": True}, transformation_ctx="S3Customercurated_node1716384247005")

# Script generated for node Join
Join_node1716384396839 = Join.apply(frame1=S3Steptrainerlanding_node1716384167237, frame2=S3Customercurated_node1716384247005, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1716384396839")

# Script generated for node Drop Fields
DropFields_node1716384427060 = DropFields.apply(frame=Join_node1716384396839, paths=[], transformation_ctx="DropFields_node1716384427060")

# Script generated for node S3 Step Trainer Trusted
S3StepTrainerTrusted_node1716384433637 = glueContext.getSink(path="s3://stedi-trusted-zone/step-trainer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="S3StepTrainerTrusted_node1716384433637")
S3StepTrainerTrusted_node1716384433637.setCatalogInfo(catalogDatabase="stedi",catalogTableName="steptrainer_trusted")
S3StepTrainerTrusted_node1716384433637.setFormat("json")
S3StepTrainerTrusted_node1716384433637.writeFrame(DropFields_node1716384427060)
job.commit()
