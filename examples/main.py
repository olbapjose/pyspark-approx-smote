#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, SQLTransformer
from pyspark.sql import SparkSession
from pyspark_approx_smote.asmote import ASMOTE


def main():
    this_folder = __file__
    spark = SparkSession.builder.getOrCreate()
    datos = spark.read.option("inferSchema", "true").option("header", "false")\
        .csv(os.path.join(this_folder, "..", "adult.csv"))\
        .withColumnRenamed("_c14", "target")\
        .dropna()

    num_cols = [c for (c, tipo) in datos.dtypes if tipo != "string"]

    string_indexer = StringIndexer(inputCol="target", outputCol="targetIndexed")
    vector_assembler = VectorAssembler(inputCols=num_cols, outputCol="features")
    sql_transformer = SQLTransformer(statement="SELECT targetIndexed, features FROM __THIS__")

    asmote = ASMOTE(
        featuresCol="features",
        labelCol="targetIndexed",
        seed=1234
    )

    datos.groupBy("target").count().show()
    pipeline = Pipeline(stages=[vector_assembler, string_indexer, sql_transformer, asmote])
    datos_transformados = pipeline.fit(datos).transform(datos)
    datos_transformados.groupBy("targetIndexed").count().show()


if __name__ == '__main__':
    main()
