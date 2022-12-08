import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1670253004107 = glueContext.create_dynamic_frame.from_catalog(
    database="db_movies",
    table_name="rating_csv",
    transformation_ctx="AWSGlueDataCatalog_node1670253004107",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1670253017800 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1670253004107,
    mappings=[
        ("userid", "long", "userid", "long"),
        ("movieid", "long", "movieid", "long"),
        ("rating", "double", "rating", "double"),
        ("timestamp", "string", "timestamp", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1670253017800",
)

# Script generated for node Amazon S3
AmazonS3_node1670253159972 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchemaApplyMapping_node1670253017800,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://datademia-cleaned-datasets/rating/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1670253159972",
)

job.commit()
