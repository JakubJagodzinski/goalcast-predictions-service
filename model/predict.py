import os
import sys
from pathlib import Path

from model.PredictionModelType import PredictionModelType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("predictions-service")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.python.worker.memory", "2g")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .getOrCreate()
)

model_paths = {
    PredictionModelType.GRADIENT_BOOST_TREE.name.lower(): Path.cwd() / "models" / "models_GBT",
    PredictionModelType.RANDOM_FOREST.name.lower(): Path.cwd() / "models" / "models_RF",
    PredictionModelType.MULTILAYER_PERCEPTRON.name.lower(): Path.cwd() / "models" / "models_MLPC",
    PredictionModelType.LINEAR_REGRESSION.name.lower(): Path.cwd() / "models" / "models_LR"
}

_models = {}


def get_model(model_name: str) -> PipelineModel:
    global _models

    model_path = model_paths.get(model_name)
    _model = _models.get(model_path)
    if _model is None:
        _model = PipelineModel.load(str(model_path))
        _models[model_path] = _model
    return _model


def predict_winner(home_team: str, away_team: str, model_name: str) -> tuple[str, float]:
    model = get_model(model_name)

    df_new = spark.createDataFrame(
        [(home_team, away_team)],
        ["home_team", "away_team"]
    )

    predictions = model.transform(df_new)

    prob_class1_udf = udf(lambda v: float(v[1]), FloatType())
    predictions_with_conf = predictions.withColumn("confidence", prob_class1_udf("probability"))

    row = predictions_with_conf.select("prediction", "confidence").collect()[0]
    winner = home_team if row['prediction'] == 1 else away_team
    confidence = row['confidence']

    return winner, confidence
