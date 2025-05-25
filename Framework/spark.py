from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession

import warnings
warnings.filterwarnings("ignore")
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

spark = SparkSession.getActiveSession()
try:
    sc = spark.sparkContext
except:
    pass


if spark is None:

    import os
    import sys
    import uuid

    os.environ.setdefault('HADOOP_CONF_DIR', '/etc/hadoop/conf')
    os.environ.setdefault('PYARROW_IGNORE_TIMEZONE', '1')
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['PYARROW_IGNORE_TIMEZONE']='1'

    # Save your AWS Credentials here

    aws_access_key_id =     '1234567890123456'
    aws_secret_access_key = '1234567890123456'
    aws_session_token =     '1234567890123456'


    spark_conf= {
        "spark.executor.cores" : "2" ,
        'spark.executor.memory' : '8g', 
        'spark.driver.memory'  : '4g',
        'spark.driver.maxResultSize' : '6g',
        
        "spark.sql.shuffle.partitions" : "8",
        "spark.log.level": "OFF", 

        # Memory management
        "spark.memory.fraction": "0.7",  # Moderate increase from default 0.6
        "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UseCompressedOops",
        "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UseCompressedOops -Dcom.amazonaws.services.s3.enableV4=true",

        # Increasing Spark Timeouts....
        "spark.network.timeout": "6000s" ,
        "spark.executor.heartbeatInterval": "60s" ,

        "spark.sql.broadcastTimeout": "600s", # Timeout for broadcast joins
        "spark.shuffle.io.connectionTimeout": "300s",
        "spark.shuffle.io.maxRetries": "5", # Increased retries for shuffle fetch failures
        "spark.rpc.askTimeout": "300s",      # Timeout for RPC requests
        "spark.rpc.lookupTimeout": "120s",     # Timeout for looking up remote endpoints

        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "5000",

        # Keeping serialization optimized
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "512m",  # Reduced from 1g to be more conservative
        "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",

        'spark.dynamicAllocation.enabled': 'false',
        'spark.hadoop.io.compression.codecs': "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.GzipCodec",
        'spark.yarn.historyServer.allowTracking': 'false', # true
        'spark.sql.parquet.fs.optimized.committer.optimization-enabled': 'false',
        'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version': '2',
        'spark.speculation': 'false',

        'spark.hadoop.fs.s3.maxRetries': '20',
        'spark.hadoop.fs.s3.maxConnections': '5000',
        'spark.hadoop.fs.s3a.maxRetries': '20',
        'spark.hadoop.fs.s3a.maxConnections': '5000',
        'spark.port.maxRetries': '1000',

        "spark.ui.showConsoleProgress": "false",

        # AWS settings
        'spark.jars.packages': 'io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.4,graphframes:graphframes:0.8.1-spark3.0-s_2.12,org.apache.spark:spark-avro_2.12:2.4.0',
        "spark.driver.extraJavaOptions":"-Dcom.amazonaws.services.s3.enableV4=true",
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider'
        
    }


    import uuid
    
    conf = SparkConf() \
        .setMaster("local[6]") \
        .setAppName(f"illorens_LOCAL_{uuid.uuid4().hex[:5]}")

    for k, v in spark_conf.items():  # type: ignore
        conf.set(k, v)

    """
    conf\
        .set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)\
        .set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)\
        .set("spark.hadoop.fs.s3a.session.token", aws_session_token)\
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")\


    conf.set('spark.hadoop.fs.s3a.access.key',   '1234567890123456')
    conf.set('spark.hadoop.fs.s3a.secret.key',   '1234567890123456')
    #                      'fs.s3a.aws.credentials.provider', 
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider') # SimpleAWSCredentialsProvider, AnonymousAWSCredentialsProvider
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
    """
    
    SparkContext.setSystemProperty("log4j.logger.org.apache.spark", "OFF")
    SparkContext.setSystemProperty("log4j.logger.org.apache.hadoop", "OFF")
    SparkContext.setSystemProperty("log4j.logger.org.eclipse.jetty", "OFF")
    context = SparkContext(conf=conf)
    context.setLogLevel(logLevel="OFF")
    spark = SparkSession(context)
    spark.sparkContext.setLogLevel("OFF")  # Suppress ERROR logs
    sc = spark.sparkContext


    spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

    hadoopConf = spark._jsc.hadoopConfiguration()  # type: ignore  # pylint: disable=protected-access  # noqa: N806
    hadoopConf.set("fs.s3.awsAccessKeyId",     aws_access_key_id)
    hadoopConf.set("fs.s3.awsSecretAccessKey", aws_secret_access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider.mapping", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    # print("spark instance is created")

# spark