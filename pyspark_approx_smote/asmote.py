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

from pyspark import keyword_only
from pyspark.ml.param.shared import HasLabelCol, HasFeaturesCol, HasSeed
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer

from pyspark_approx_smote.asmote_params import ASMOTEParams


class ASMOTE(JavaTransformer, ASMOTEParams, HasLabelCol, HasFeaturesCol, HasSeed, JavaMLReadable,
             JavaMLWritable):

    """
    A PySpark wrapper of org.apache.spark.ml.instance.ASMOTE with Maven coordinates mjuez:approx-smote:1.1.0

    Examples
    --------
    >>> from pyspark.ml.feature import VectorAssembler, SQLTransformer
    >>> from pyspark.ml import Pipeline
    >>> rand = pyspark.sql.functions.rand(seed=1)
    >>> df = spark.range(1, 1001).select(rand.alias("v1"), rand.alias("v2"), (rand < 0.2).cast("double").alias("target"))
    >>> df.groupBy("target").count().show()
    +------+-----+
    |target|count|
    +------+-----+
    |   0.0|  774|
    |   1.0|  226|
    +------+-----+
    >>> vector_assembler = VectorAssembler(inputCols=["v1", "v2"], outputCol="features")
    >>> sql_transformer = SQLTransformer(statement="SELECT target, features FROM __THIS__")
    >>> asmote = ASMOTE(featuresCol="features", labelCol="target", topTreeSize=100)
    >>> pipeline = Pipeline(stages=[vector_assembler, sql_transformer, asmote])
    >>> oversampled = pipeline.fit(df).transform(df)
    >>> oversampled.groupBy("target").count().show()   # the number of examples of the minority class has doubled
    +------+-----+
    |target|count|
    +------+-----+
    |   0.0|  774|
    |   1.0|  452|
    +------+-----+
    """

    @keyword_only
    def __init__(self, *,
                 featuresCol="features",
                 labelCol="label",
                 seed=0,
                 k=5,
                 percOver=100,
                 topTreeSize=1000,
                 topTreeLeafSize=10,
                 subTreeLeafSize=30,
                 bufferSize=-1.0,
                 bufferSizeSampleSize=[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
                 balanceThreshold=0.7,
                 maxNeighbors=float("inf")
                 ):
        """
        __init__(self, \\*, featuresCol="features",
                 labelCol="label",
                 seed=0,
                 k=5,
                 percOver=100,
                 topTreeSize=1000,
                 topTreeLeafSize=10,
                 subTreeLeafSize=30,
                 bufferSize=-1.0,
                 bufferSizeSampleSize=[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
                 balanceThreshold=0.7,
                 maxNeighbors=float("inf"))
        """
        super(ASMOTE, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.instance.ASMOTE", self.uid)
        self._setDefault(
            featuresCol="features", labelCol="label", seed=0,
            k=5, percOver=100, topTreeSize=1000, topTreeLeafSize=10, subTreeLeafSize=30, bufferSize=-1.0,
            bufferSizeSampleSize=[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
            balanceThreshold=0.7, maxNeighbors=float("inf")
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, *,
                  featuresCol="features",
                  labelCol="label",
                  seed=0,
                  k=5,
                  percOver=100,
                  topTreeSize=1000,
                  topTreeLeafSize=10,
                  subTreeLeafSize=30,
                  bufferSize=-1.0,
                  bufferSizeSampleSize=[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
                  balanceThreshold=0.7,
                  maxNeighbors=float("inf")
                  ):
        """
        setParams(self, \\*, inputCols=None, outputCol=None, handleInvalid="error")
        Sets params for this VectorAssembler.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setFeaturesCol(self, value):
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    def setLabelCol(self, value):
        """
        Sets the value of :py:attr:`labelCol`.
        """
        return self._set(labelCol=value)

    def setSeed(self, value):
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    def setPercOver(self, value):
        """
        Sets the value of :py:attr:`percOver`.
        """
        return self._set(percOver=value)

    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    def setMaxDistance(self, value):
        """
        Sets the value of :py:attr:`maxNeighbors`.
        """
        return self._set(maxNeighbors=value)

    def setBufferSize(self, value):
        """
        Sets the value of :py:attr:`bufferSize`.
        """
        return self._set(bufferSize=value)

    def setTopTreeSize(self, value):
        """
        Sets the value of :py:attr:`topTreeSize`.
        """
        return self._set(topTreeSize=value)

    def setTopTreeLeafSize(self, value):
        """
        Sets the value of :py:attr:`topTreeLeafSize`.
        """
        return self._set(topTreeLeafSize=value)

    def setSubTreeLeafSize(self, value):
        """
        Sets the value of :py:attr:`subTreeLeafSize`.
        """
        return self._set(subTreeLeafSize=value)

    def setBufferSizeSampleSizes(self, value):
        """
        Sets the value of :py:attr:`bufferSizeSampleSize`.
        """
        return self._set(bufferSizeSampleSize=value)

    def setBalanceThreshold(self, value):
        """
        Sets the value of :py:attr:`balanceThreshold`.
        """
        return self._set(balanceThreshold=value)
