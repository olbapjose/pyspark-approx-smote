# pyspark-approx-smote
Pyspark wrapper of the scala (Spark) version of Approx SMOTE

The original Spark-based version of Approx SMOTE written in Scala can be
found [here](https://github.com/mjuez/approx-smote). The Maven coordinates are `mjuez:approx-smote:jar:1.1.0` 
([available here](https://mvnrepository.com/artifact/mjuez/approx-smote)).


For the wrapper to work, the JAR file must be present in the Spark classpath.

### Installing `pyspark-approx-smote` from this repo:

```shell
git clone https://github.com/olbapjose/pyspark-approx-smote
cd pyspark-approx-smote
pip install -e .
```

### Use example:

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, SQLTransformer
from pyspark_approx_smote.asmote import ASMOTE

datos = spark.read.option("inferSchema", "true").option("header", "false")\
    .csv("adult.csv")\
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

```



