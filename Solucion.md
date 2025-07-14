Aquí se detalla la arquitectura e implementación de una solución serverless escalable para las órdenes de Duff Beer. Se diseña un pipeline automatizado de punta a punta que se gatilla al subir nuevos archivos a un bucket de s3, el resultado es la disposición de los datos vía API.

La solución se puede desplegar via CloudFormation con estas 3 etapas.

Fase 1: Creación de Buckets en S3 (buckets.yaml)

El archivo despliega la creación de 3 buckets:
* duff-orders-raw: Bucket de aterrizaje para los archivos raw en formato CSV.
* duff-orders-processed: Bucket donde llegará el archivo ya procesado en formato parquet y particionado por fecha de procesamiento para optimizar el rendimiento de las consultas.
* aws-project-artifacts-interview: Bucket que almacena los scripts de funciones lambdas y glue jobs necesarios para el despliegue.

Disclaimer: Los nombres de los buckets son únicos a nivel global por lo que tendrán que cambiar los nombres para el despliegue.

Fase 2: Procesamiento ETL
Esta fase abarca la lógica principal de procesamiento y transformación de datos, y se despliega mediante el archivo main-etl.yaml
Componentes:
* Validate_csv: Funcion Lambda que valida el tamaño del archivo, esto nos es util debido a que usaremos distintas estrategias de procesamiento si el archivo es menor o mayor a 100mb (Lambda para archivos pequeños, glue para archivos grandes).
Transform-data: Funcion que realiza campos calculados sobre la tabla inicial, como por ejemplo precio total. Elimina duplicados y chequea errores por archivo valido o no valido dependiendo si tiene columnas o filas nulas. Ademas,le asigna el schema definidio en el data catalog y guarda el archivo en formato parquet y particionado para mejor performance a la hora de consultar la data.

* TriggerGlueJob: Lambda que actúa como trigger de proceso Glue para archivos sobre 100mb.

*Query_Athena:  Lambda ejecuta consultas SQL en AWS Athena sobre los datos ya procesados. Su objetivo final es exponer los resultados de las consultas a través de un API Gateway.

*Orquestación con AWS Step Functions: Es el gestor de la orquestación del ETL, comunica los glue y lambda además de tomar desiciones desde los output de cada uno.


Glue: 
Duff_transform_glue_job: Realiza la misma función del lambda Transform-data pero con Pyspark para computación distribuida en archivos grandes.

Data Catalog:
Se define la base de datos db_duff_orders y la tabla tbl_duff_orders que contendrá los datos ya procesados. Se define el schema de la tabla para que tanto glue como athena puedan consultarlo.

API Gateway:
Este servicio establece el punto de acceso API que se conecta con la función Lambda Query_Athena.

3.- Data Modeling
Fase final del pipeline, se enfoca en transformar los datos ya procesados en nuestra tabla y darle estructura de estrella con una tabla de hechos y 4 dimensiones.
En este ultimo paso se crea un Glue job llamado duff_data_models.py que crea las tablas dimensionas y la tabla hecho definidas en en el siguiente diagrama:
*foto*

Finalmente las consultas a la api pueden hacerse a esta url utilizando postman : *url*
