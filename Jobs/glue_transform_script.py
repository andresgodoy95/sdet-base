import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job 
from awsglue.gluetypes import *
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs
import sys
from awsglue.utils import getResolvedOptions

# Expecting --input_path from job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path'])
input_path = args['input_path']

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col
    from pyspark.sql.functions import col, current_date

    # Get the first (and likely only) DynamicFrame from the collection
    dynamic_frame = dfc.select(list(dfc.keys())[0])

    # Convert to DataFrame
    df = dynamic_frame.toDF()


    # Perform calculation and cast to double
    df = df.withColumn(
        "total_price",
        (col("product_price").cast("double") * col("product_ccf").cast("double"))
    )

    df = df.withColumn("pa_date", current_date().cast("string"))
    # Convert back to DynamicFrame
    transformed_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed_df")

    # Wrap in a new DynamicFrameCollection and return
    return DynamicFrameCollection({"CustomTransform0": transformed_dynamic_frame}, glueContext)
def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str) :
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1752363259443 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [input_path], "recurse": True}, transformation_ctx="AmazonS3_node1752363259443")

# After loading the data
print(f"Reading from path: {input_path}")
AmazonS3_node1752363259443.printSchema()

# Try showing first 5 records
df = AmazonS3_node1752363259443.toDF()
df.show(5)


# Script generated for node Drop Null Fields
DropNullFields_node1752363283093 = drop_nulls(glueContext, frame=AmazonS3_node1752363259443, nullStringSet={"", "null"}, nullIntegerSet={}, transformation_ctx="DropNullFields_node1752363283093")

# Script generated for node Drop Duplicates
DropDuplicates_node1752363284913 =  DynamicFrame.fromDF(DropNullFields_node1752363283093.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1752363284913")

# Script generated for node Custom Transform
CustomTransform_node1752363989969 = MyTransform(glueContext, DynamicFrameCollection({"DropDuplicates_node1752363284913": DropDuplicates_node1752363284913}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1752367174212 = SelectFromCollection.apply(dfc=CustomTransform_node1752363989969, key=list(CustomTransform_node1752363989969.keys())[0], transformation_ctx="SelectFromCollection_node1752367174212")

# Script generated for node Change Schema
ChangeSchema_node1752364508964 = ApplyMapping.apply(frame=SelectFromCollection_node1752367174212, mappings=[("client_id", "string", "client_id", "int"), ("client_name", "string", "client_name", "string"), ("order_id", "string", "order_id", "int"), ("product_id", "string", "product_id", "int"), ("product_description", "string", "product_description", "string"), ("product_price", "string", "product_price", "double"), ("product_ccf", "string", "product_ccf", "double"), ("product_volume", "string", "product_volume", "float"), ("point_of_sale_channel", "string", "point_of_sale_channel", "string"), ("status", "string", "status", "string"), ("total_price", "double", "total_price", "double"), ("pa_date", "string", "pa_date", "string")], transformation_ctx="ChangeSchema_node1752364508964")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1752364508964, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752363235973", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1752363350136 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1752364508964, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aws-arkho-curated/tbl_Duff_prod/", "partitionKeys": ["pa_date"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1752363350136")

job.commit()