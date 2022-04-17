
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

from pyspark.ml.param import *

class ASMOTEParams(Params):
    """
    Mixin for param inputCols: input column names.
    """

    k = Param(Params._dummy(), "k",  "Number of nearest neighbours, preferably an odd number (>=3).",
                      typeConverter=TypeConverters.toInt)

    percOver = Param(Params._dummy(), "percOver",  "Oversampling percentage as integer (>=1 and <=100).",
                     typeConverter=TypeConverters.toInt)

    maxNeighbors = Param(Params._dummy(), "maxNeighbors",  "maximum distance to find neighbors",
                         typeConverter=TypeConverters.toFloat)

    bufferSize = Param(Params._dummy(), "bufferSize",
                       "size of buffer used to construct spill trees and top-level tree search (>=-1)",
                       typeConverter=TypeConverters.toFloat)

    topTreeSize = Param(Params._dummy(), "topTreeSize",
                        "Number of points to sample for top-level tree (KNN) (>0)",
                        typeConverter=TypeConverters.toInt)

    topTreeLeafSize = Param(Params._dummy(), "topTreeLeafSize",
                            "Number of points at which to switch to brute-force for top-level tree (>0)",
                            typeConverter=TypeConverters.toInt)

    subTreeLeafSize = Param(Params._dummy(), "subTreeLeafSize",
                      "number of points at which to switch to brute-force for distributed sub-trees (>0)",
                      typeConverter=TypeConverters.toInt)

    bufferSizeSampleSize = Param(Params._dummy(), "bufferSizeSampleSize",
                      "array of sample sizes to take when estimating buffer size (length > 1 and each >0)",
                      typeConverter=TypeConverters.toListInt)

    balanceThreshold = Param(Params._dummy(), "balanceThreshold",
                      "fraction of total points at which spill tree reverts back to metric tree if either child contains more points (in [0, 1]",
                      typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(ASMOTEParams, self).__init__()
        self._setDefault(
            k=5, percOver=100, topTreeSize=1000, topTreeLeafSize=10, subTreeLeafSize=30, bufferSize=-1.0,
            bufferSizeSampleSize=[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
            balanceThreshold=0.7, maxNeighbors=float("inf"))

    def getPercOver(self):
        """
        Gets the value of percOver or its default value.
        """
        return self.getOrDefault(self.percOver)

    def getK(self):
        """
        Gets the value of k (for the internal approximate k-NN) or its default value.
        """
        return self.getOrDefault(self.k)

    def getMaxDistance(self):
        """
        Gets the value of maxNeighbors or its default value.
        """
        return self.getOrDefault(self.maxNeighbors)

    def getBufferSize(self):
        """
        Gets the value of bufferSize or its default value.
        """
        return self.getOrDefault(self.bufferSize)

    def getTopTreeSize(self):
        """
        Gets the value of topTreeSize or its default value.
        """
        return self.getOrDefault(self.topTreeSize)

    def getTopTreeLeafSize(self):
        """
        Gets the value of topTreeLeafSize or its default value.
        """
        return self.getOrDefault(self.topTreeLeafSize)

    def getSubTreeLeafSize(self):
        """
        Gets the value of subTreeLeafSize or its default value.
        """
        return self.getOrDefault(self.subTreeLeafSize)

    def getBufferSizeSampleSizes(self):
        """
        Gets the value of bufferSizeSampleSize or its default value.
        """
        return self.getOrDefault(self.bufferSizeSampleSize)

    def getBalanceThreshold(self):
        """
        Gets the value of balanceThreshold or its default value.
        """
        return self.getOrDefault(self.balanceThreshold)
