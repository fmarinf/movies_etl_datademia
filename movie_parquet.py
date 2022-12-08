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
AWSGlueDataCatalog_node1670251918274 = glueContext.create_dynamic_frame.from_catalog(
    database="db_movies",
    table_name="movie_csv",
    transformation_ctx="AWSGlueDataCatalog_node1670251918274",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1670252060331 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1670251918274,
    mappings=[
        ("movieid", "long", "movieid", "long"),
        ("title", "string", "title", "string"),
        ("genres", "string", "genres", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1670252060331",
)

# Script generated for node Amazon S3
AmazonS3_node1670252276700 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchemaApplyMapping_node1670252060331,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://datademia-cleaned-datasets/movie/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1670252276700",
)

job.commit()
