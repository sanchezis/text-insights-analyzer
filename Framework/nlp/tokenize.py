import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

import string
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark import SparkFiles

from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.pipeline import Pipeline
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram, HashingTF, IDF, Word2Vec, RegexTokenizer

from pyspark.sql.dataframe import Column
from typing import Union, List

import numpy as np

__spark__ = SparkSession.getActiveSession()


# Custom Tokenizer Class
class CustomTokenizer(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):

    def __init__(self, inputCol=None, outputCol=None):
        super(CustomTokenizer, self).__init__()
        self._setDefault(inputCol=inputCol, outputCol=outputCol)
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self, dataset: DataFrame) -> DataFrame:
        
        @F.udf(ArrayType(StringType()))
        def tokenize_and_remove_duplicates(text):
            if text is None:
                return []
            # Tokenize by splitting on whitespace
            tokens = text.split()
            # Remove duplicates while preserving order
            unique_tokens = []
            seen = set()
            for token in tokens:
                if token not in seen:
                    seen.add(token)
                    unique_tokens.append(token)
            return unique_tokens

        # Apply the UDF to the input column and create the output column
        return dataset.withColumn(
                                    self.outputCol, 
                                    tokenize_and_remove_duplicates(dataset[self.inputCol])
                                )

@F.udf(returnType=StringType())
def clean_text(text):
    import re
    if text:
        return re.sub(r'[^A-Za-z0-9\s]', '', text).lower()
    return None

def extract_tokens(self, column:Union[str, Column], fn:callable = clean_text) -> pyspark.sql.DataFrame :
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram, HashingTF, IDF, Word2Vec, RegexTokenizer

    cleaned =  f"__cleaned__"

    return self\
        .withColumn(cleaned, F.regexp_replace(pattern=r"(?i)i'm",  replacement='i am',    string=column)) \
        .withColumn(cleaned, F.regexp_replace(pattern=r"(?i)i'll", replacement='i will',  string=cleaned)) \
        .withColumn(cleaned, F.regexp_replace(pattern=r"(?i)'d", replacement=' would',  string=cleaned)) \
        .withColumn(cleaned, F.regexp_replace(pattern=r"(?i)i've", replacement='i have',  string=cleaned)) \
        .withColumn(cleaned, F.regexp_replace(pattern=r"(?i)'re",  replacement=' are',    string=cleaned)) \
        .withColumn(cleaned, F.regexp_replace(pattern=r"(?i)'s",  replacement='',         string=cleaned)) \
        .withColumn(cleaned, F.regexp_replace(pattern=r"(?i)'ve", replacement=' have',    string=cleaned)) \
        .withColumn(cleaned, fn(cleaned))

def tokenize(self, language:str="english", stopwords=[], cleaned =  f"__cleaned__", embedding_dimension:int=100) -> pyspark.sql.DataFrame :
    tokens =   f"__tokens__"
    filtered = f"__filtered_tokens__"
    ngrams   = f"__ngrams__"
    hashing  = f"__tf_hashing__"
    tf_idf   = f"__tf_idf__"

    stages = []

    tokenizer = CustomTokenizer(inputCol=cleaned, outputCol=tokens)
    stages.append(tokenizer)
    
    stopword_remover = StopWordsRemover(
        inputCol=tokens, 
        outputCol=filtered, 
        stopWords=StopWordsRemover.loadDefaultStopWords(language=language) + stopwords
        )
    stages.append(stopword_remover)

    ngram = NGram(n=2, inputCol=filtered, outputCol=ngrams)
    stages.append(ngram)

    preprocessing_pipeline = Pipeline(stages=stages)

    processed_df = preprocessing_pipeline.fit(self).transform(self).drop(*[hashing, tokens])
    return processed_df

