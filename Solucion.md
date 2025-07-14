Aquí se detalla la arquitectura e implementación de una solución serverless escalable para las órdenes de Duff Beer. Se diseña un pipeline automatizado de punta a punta que se gatilla al subir nuevos archivos a un bucket de s3, el resultado es la disposición de los datos vía API.

La solución se puede desplegar via CloudFormation con estas 3 etapas.

Fase 1: Creación de Buckets en S3 (buckets.yaml)

El archivo despliega la creación de 3 buckets:
* duff-orders-raw: Bucket de aterrizaje para los archivos raw en formato CSV.
* duff-orders-processed: Bucket donde llegará el archivo ya procesado en formato parquet y particionado por fecha de procesamiento para optimizar el rendimiento de las consultas.
* aws-project-artifacts-interview: Bucket que almacena los scripts de funciones lambdas y glue jobs necesarios para el despliegue.

Disclaimer: Los nombres de los buckets son únicos a nivel global por lo que tendrán que cambiar los nombres para el despliegue.

Fase 2: Procesamiento ETL
Esta fase abarca la lógica principal de procesamiento y transformación de datos, y se despliega mediante el archivo main-etl.yaml.
Antes de desplegar el .yaml, cargar los jobs y lambda scripts al bucket, sin carpeta solo los script.
Componentes:
* Validate_csv: Funcion Lambda que valida el tamaño del archivo, esto nos es util debido a que usaremos distintas estrategias de procesamiento si el archivo es menor o mayor a 100mb (Lambda para archivos pequeños, glue para archivos grandes).
Transform-data: Funcion que realiza campos calculados sobre la tabla inicial, como por ejemplo precio total. Elimina duplicados y chequea errores por archivo valido o no valido dependiendo si tiene columnas o filas nulas. Ademas,le asigna el schema definidio en el data catalog y guarda el archivo en formato parquet y particionado para mejor performance a la hora de consultar la data.

* TriggerGlueJob: Lambda que actúa como trigger de proceso Glue para archivos sobre 100mb.

* Query_Athena:  Lambda ejecuta consultas SQL en AWS Athena sobre los datos ya procesados. Su objetivo final es exponer los resultados de las consultas a través de un API Gateway.

* Orquestación con AWS Step Functions: Es el gestor de la orquestación del ETL, comunica los glue y lambda además de tomar desiciones desde los output de cada uno.
<img width="922" height="693" alt="image" src="https://github.com/user-attachments/assets/402677ce-ab40-4e61-8e88-695ad71d54cd" />



Glue: 
Duff_transform_glue_job: Realiza la misma función del lambda Transform-data pero con Pyspark para computación distribuida en archivos grandes.

Data Catalog:
Se define la base de datos db_duff_orders y la tabla tbl_duff_orders que contendrá los datos ya procesados. Se define el schema de la tabla para que tanto glue como athena puedan consultarlo.

API Gateway:
Este servicio establece el punto de acceso API que se conecta con la función Lambda Query_Athena.

s3 Trigger: Para ejecutar la stepfunction cada vez que llega un nuevo archivo, hay que ir a propiedades del bucket y habilitar las notificaciones de CloudWatch, luego crear una regla con nuestro bucket y la aplicacion de destino sería nuestra Step Function. Asegurarse de que el rol IAM de la stepfunction tenga permisos sobre el bucket.

3.- Data Modeling
Fase final del pipeline, se enfoca en transformar los datos ya procesados en nuestra tabla y darle estructura de estrella con una tabla de hechos y 4 dimensiones.
En este ultimo paso se crea un Glue job llamado duff_data_models.py que crea las tablas dimensionas y la tabla hecho definidas en en el siguiente diagrama:
<img width="978" height="720" alt="image" src="https://github.com/user-attachments/assets/38555a35-027f-40ec-b5cc-6c08a2e5df0f" />

Tablas de Dimensión:

dim_clients: Contiene información única de cada cliente.

dim_products: Detalles específicos de cada producto de Duff.

dim_status: Define los estados posibles de una orden con identificadores únicos.

dim_channels: Identifica los canales de venta con sus respectivos identificadores únicos.

Tabla de Hechos:

fact_orders: Es el centro de nuestro modelo analítico. Contiene las métricas clave de cada orden (ej. cantidad, precio total) y se vincula a las tablas de dimensión mediante claves subrogadas (client_id, product_id, status_id, channel_id). Esta tabla está particionada por la fecha de la orden (pa_date) para optimizar las consultas de series temporales e históricas.

Finalmente, los campos destacados en amarillo son posibles campos para agregar en el futuro para tener mas información.

La API Gateway se encuentra funcionando en el siguiente enlace: https://l69xgm3e8j.execute-api.us-east-1.amazonaws.com/prod/client-orders?
Puedes ejecutarla con los siguientes parametros:
- Revisar las ordenes:
  {"resource":"client_orders"}
- Revisar status
  {"resource":"status_summary"}
- Ordenes de clientes por client_id
  {"resource":"orders_by_client",
  "client_id":1001}

  <img width="1763" height="709" alt="image" src="https://github.com/user-attachments/assets/80875dd9-2973-41d0-a934-45bef2845062" />


Oportunidades de Mejora:
Parametrizar el stack de despliegue para elegir los nombres de los buckets de entrada y salida, ademas de parametrizar el nombre de la base y tablas.
Considerar guardar schemas en dynamoDB en caso de tener schemas que cambien en el tiempo. Otra solucion es agregar una columna "vigente".
-Agregar una herramienta de visualizacion como Quicksight para mostrar los reportes como front-end en vez de la respuesta con formato plano.

