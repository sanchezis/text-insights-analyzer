try:
    import solvessl
except:
    pass   

import numpy as np
import pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items

import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime
import random

__spark__ = pyspark.sql.SparkSession.getActiveSession() # pyspark.sql.SparkSession.builder.getOrCreate()


def telecom_200k(): 
    import os
    from Framework.utils.web import download_file

    dataset_url = "https://huggingface.co/datasets/talkmap/telecom-conversation-corpus/resolve/main/telecom_200k.csv"
    local_file = dataset_url.split('/')[-1]
    
    # Download the file if it doesn't exist
    if not os.path.exists(local_file):
        try:
            download_file(dataset_url, local_file)
        except Exception as e:
            # print(f"Error downloading file: {e}")
            return None

    # infer schema with 10% of the data
    schema_df = (__spark__.read.format("csv") 
                    .option("header", "true") 
                    .option("inferSchema", "true") 
                    .option("samplingRatio", "0.1")   # Only sample 10% to infer schema
                    .load(local_file)
                )
    
    schema = schema_df.schema
    
    # load with the known schema and partitioning
    df = ( __spark__.read.format("csv") 
            .option("header", "true") 
            .schema(schema)  # Use pre-inferred schema instead of inferring again
            .option("mode", "DROPMALFORMED") 
            .option("maxColumns", "30000") 
            .option("maxCharsPerColumn", "100000") 
            .load(local_file)
            )

    return df    
