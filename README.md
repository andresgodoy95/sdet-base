rimero, creamos 2 buckets, uno para la entrada de los archivos y otro para la salida de los archivos ya procesados. Los llamaremos:
* duff-orders-raw
* duff-orders-processed

Cargamos una carpeta con el archivo csv que contiene los datos de Duff a duff-orders-raw.

Luego de revisar la estructura de la tabla, crearemos un proceso lambda llamado validate_csv para validar el tamaño del archivo, esto con el objetivo de elegir nuestra estrategia de procesamiento. Si el archivo es liviano <10mb puede procesarse como un único archivo con lambda, si es de mayor tamaño pero no mayor a 100mb, podemos utilizar técnicas de batch processing dentro del lambda para procesar por partes el archivo. Finalmente si el archivo es muy grande tendremos que recurrir a glue para procesarlo. 
Esto lo definiremos en nuestro algoritmo utilizando Step functions.

Antes de seguir con la arquitectura del proceso, definiremos 4 roles IAM para cada servicio utilizado (Stepfunctions,Lambda,Glue y API Gateway).

Estos roles y la estructura completa del proceso están disponibles como archivos yaml y pueden ser cargados a AWS mediante CloudFormation (IaC).

Finalmente, la estructura de la step function es la siguiente:
foto
validateCSV: chequea el tamaño del archivo para marcar asi su ruta. Ademas, revisa si los campos que vienen son los correctos.
Check size: Si el tamaño del archivo supera los 100mb es clasificado como large =True y un glue job lo procesará, si es menor a eso lambda lo procesará. Si el archivo no cumple con las especificaciones de columnas es catalogado como no valido y sale del proceso.
TransformData: Aquí se hacen las transformaciones con lambda, si el archivo es mayor a 10mb se divide en chuncks y se procesa. Se calcula la columna de total price y se eliminan nulos y duplicados.
TriggerGlueJob: Lambda que gatilla el glue job con las mismas transformaciones antes mencionadas.
QueryAthena: Lambda que ejecuta las consultas y se comunica con API Gateway para luego poder hacer consultas al endpoint. Esta funcion tambien carga las particiones de las nuevas cargas a la tabla y asi no usar el crawler.

Ahora agregamos el trigger a la llegada del archivo, para esto nos dirigimos al bucket de llegada y activamos las notificaciones de EventBridge. Posterior a esto configuramos una regla de Eventbridge con destino nuestra stepfunction, configuramos el rol IAM de nuestra stepfunction y listo.

Una vez configurado, el payload que nos entrega el evento es el siguiente:
{
  "version": "0",
  "id": "ba8fe66b-09f9-f98f-8395-96f5a21fe866",
  "detail-type": "Object Created",
  "source": "aws.s3",
  "account": "769148412279",
  "time": "2025-07-14T00:53:10Z",
  "region": "us-east-1",
  "resources": [
    "arn:aws:s3:::duff-orders-raw"
  ],
  "detail": {
    "version": "0",
    "bucket": {
      "name": "duff-orders-raw"
    },
    "object": {
      "key": "Duff_data/raw/tbl_duff_orders.csv",
      "size": 466,
      "etag": "fd41bdc1c61bcf8576147e3b873cb743",
      "sequencer": "00687454F676495266"
    },
    "request-id": "CEQC1K04W6DJGAX0",
    "requester": "769148412279",
    "source-ip-address": "190.196.48.238",
    "reason": "CopyObject"
  }
}

Con esto podemos obtener dinámicamente las variables que necesitamos para el proceso.


