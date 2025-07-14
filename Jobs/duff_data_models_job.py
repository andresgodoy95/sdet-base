import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame # Importar DynamicFrame

# --- 1. Inicialización de Glue Job ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 2. Leer los datos procesados existentes (tbl_duff_orders) ---
print("Reading source data from db_duff_orders.tbl_duff_orders...")
source_df = glueContext.create_dynamic_frame.from_catalog(
    database="db_duff_orders",
    table_name="tbl_duff_orders",
    transformation_ctx="source_data"
).toDF() # Convertir a Spark DataFrame para usar funciones de Spark SQL

print("--- SCHEMA OF SOURCE_DF ---")
source_df.printSchema() # This will print the schema to the job logs
print(f"Number of rows in source_df: {source_df.count()}") # ADD THIS LINE
print("--- END SCHEMA OF SOURCE_DF ---")

source_df.cache() # Cachear el DataFrame si se va a usar múltiples veces

# --- 3. Crear las Tablas de Dimensión ---

# 3.1. Dimensión de Clientes (dim_clients)
print("Creating dim_clients...")
dim_clients_df = source_df.select("client_id", "client_name").dropDuplicates(["client_id"])

print("--- SCHEMA OF dim_clients_df BEFORE WRITE ---")
dim_clients_df.printSchema()
print(f"Number of rows in dim_clients_df: {dim_clients_df.count()}")
print("--- END SCHEMA OF dim_clients_df BEFORE WRITE ---")

# *** CONVERTIR A DYNAMICFRAME ANTES DE ESCRIBIR ***
clients_dynamic_frame = DynamicFrame.fromDF(
    dim_clients_df.repartition(1), # Repartitionar el DataFrame Spark
    glueContext,
    "clients_dynamic_frame_conversion" # Nombre del contexto de transformación
)

glueContext.write_dynamic_frame.from_options(
    frame = clients_dynamic_frame, # Ahora es un DynamicFrame
    connection_type = "s3",
    connection_options = {"path": "s3://duff-orders-processed/dim_clients/"},
    format = "parquet",
    transformation_ctx = "clients_dim_write"
)
print("dim_clients created.")
dim_clients_df.cache() # Cachear para joins futuros

# 3.2. Dimensión de Productos (dim_products)
print("Creating dim_products...")
dim_products_df = source_df.select("product_id", "product_description", "product_price", "product_volume").dropDuplicates(["product_id"])

# *** CONVERTIR A DYNAMICFRAME ANTES DE ESCRIBIR ***
products_dynamic_frame = DynamicFrame.fromDF(
    dim_products_df.repartition(1), # Repartitionar el DataFrame Spark
    glueContext,
    "products_dynamic_frame_conversion"
)

glueContext.write_dynamic_frame.from_options(
    frame = products_dynamic_frame, # Ahora es un DynamicFrame
    connection_type = "s3",
    connection_options = {"path": "s3://duff-orders-processed/dim_products/"},
    format = "parquet",
    transformation_ctx = "products_dim_write"
)
print("dim_products created.")
dim_products_df.cache()

# 3.3. Dimensión de Estados (dim_status) - Creando una clave subrogada
print("Creating dim_status...")
status_df = source_df.select("status").dropDuplicates(["status"])
window_spec_status = Window.orderBy("status")
dim_status_df = status_df.withColumn("status_id", row_number().over(window_spec_status)).select("status_id", "status")

# *** CONVERTIR A DYNAMICFRAME ANTES DE ESCRIBIR ***
status_dynamic_frame = DynamicFrame.fromDF(
    dim_status_df.repartition(1), # Repartitionar el DataFrame Spark
    glueContext,
    "status_dynamic_frame_conversion"
)

glueContext.write_dynamic_frame.from_options(
    frame = status_dynamic_frame, # Ahora es un DynamicFrame
    connection_type = "s3",
    connection_options = {"path": "s3://duff-orders-processed/dim_status/"},
    format = "parquet",
    transformation_ctx = "status_dim_write"
)
print("dim_status created.")
dim_status_df.cache()

# 3.4. Dimensión de Canales (dim_channels) - Creando una clave subrogada
print("Creating dim_channels...")
channels_df = source_df.select("point_of_sale_channel").dropDuplicates(["point_of_sale_channel"])
window_spec_channel = Window.orderBy("point_of_sale_channel")
dim_channels_df = channels_df.withColumn("channel_id", row_number().over(window_spec_channel)).select("channel_id", "point_of_sale_channel")

# *** CONVERTIR A DYNAMICFRAME ANTES DE ESCRIBIR ***
channels_dynamic_frame = DynamicFrame.fromDF(
    dim_channels_df.repartition(1), # Repartitionar el DataFrame Spark
    glueContext,
    "channels_dynamic_frame_conversion"
)

glueContext.write_dynamic_frame.from_options(
    frame = channels_dynamic_frame, # Ahora es un DynamicFrame
    connection_type = "s3",
    connection_options = {"path": "s3://duff-orders-processed/dim_channels/"},
    format = "parquet",
    transformation_ctx = "channels_dim_write"
)
print("dim_channels created.")
dim_channels_df.cache()


# --- 4. Crear la Tabla de Hechos (fact_orders) ---
print("Creating fact_orders...")

# Seleccionar solo las columnas necesarias para la tabla de hechos, incluyendo 'pa_date'
fact_orders_initial_df = source_df.select(
    "order_id",
    "client_id",
    "product_id",
    "status", # Para el join
    "point_of_sale_channel", # Para el join
    "product_ccf",
    "total_price",
    "pa_date" # pa_date DEBE ESTAR aquí para que pueda ser usada en partitionKeys
)

# Unir con la dimensión de estados para obtener status_id
fact_orders_joined_df = fact_orders_initial_df.join(dim_status_df, on="status", how="left").drop("status")

# Unir con la dimensión de canales para obtener channel_id
fact_orders_final_df = fact_orders_joined_df.join(dim_channels_df, on="point_of_sale_channel", how="left").drop("point_of_sale_channel")

# *** CONVERTIR A DYNAMICFRAME ANTES DE ESCRIBIR ***
# La tabla de hechos también debe convertirse
fact_orders_dynamic_frame = DynamicFrame.fromDF(
    fact_orders_final_df, # No necesitas repartition(1) si quieres múltiples archivos por partición
    glueContext,
    "fact_orders_dynamic_frame_conversion"
)

glueContext.write_dynamic_frame.from_options(
    frame = fact_orders_dynamic_frame, # Ahora es un DynamicFrame
    connection_type = "s3",
    connection_options = {"path": "s3://duff-orders-processed/fact_orders/", "partitionKeys": ["pa_date"]},
    format = "parquet",
    transformation_ctx = "fact_orders_write"
)
print("fact_orders created.")

# Limpiar cache
source_df.unpersist()
dim_clients_df.unpersist()
dim_products_df.unpersist()
dim_status_df.unpersist()
dim_channels_df.unpersist()

job.commit()
print("Glue Job finished successfully.")