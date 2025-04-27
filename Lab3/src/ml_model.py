import mlflow
import mlflow.spark
import logging
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)

class SalesPredictor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.gold_path = "/app/data/gold/sales_agg"
        self.experiment_name = "SalesPrediction"

        self.mlflow_uri = "http://0.0.0.0:5000"
        mlflow.set_tracking_uri(self.mlflow_uri)
        
        if not mlflow.get_experiment_by_name(self.experiment_name):
            mlflow.create_experiment(self.experiment_name)
        mlflow.set_experiment(self.experiment_name)

    def train_and_evaluate(self, num_trees=100, max_depth=5):
        try:
            df = self.spark.read.format("delta").load(self.gold_path)
            train_df, test_df = self._prepare_data(df)
            rf = RandomForestRegressor(
                featuresCol="features",
                labelCol="TotalUnitsSold",
                numTrees=num_trees,
                maxDepth=max_depth,
                seed=42
            )
            pipeline = self._create_pipeline(rf)
            
            with mlflow.start_run():
                mlflow.log_param("num_trees", num_trees)
                mlflow.log_param("max_depth", max_depth)
                mlflow.log_param("data_path", self.gold_path)
                
                model = pipeline.fit(train_df)

                mlflow.spark.log_model(model, "spark-random-forest-model")
                mlflow.log_artifact("/app/logs/app.log")
                
                predictions = model.transform(test_df)
                metrics = self._evaluate_model(predictions)
                
                mlflow.log_metrics(metrics)
                logger.info(f"Model metrics: {metrics}")
                
            logger.info("Model training and evaluation completed successfully")
            
        except Exception as e:
            logger.error(f"Error in model training: {str(e)}", exc_info=True)
            raise

    def _prepare_data(self, df):
        df = df.filter(col("TotalUnitsSold") > 0)
        
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        return train_df, test_df

    def _create_pipeline(self, model):
        region_indexer = StringIndexer(inputCol="Region", outputCol="RegionIndex")
        item_type_indexer = StringIndexer(inputCol="Item_Type", outputCol="ItemTypeIndex")
        
        encoder = OneHotEncoder(
            inputCols=["RegionIndex", "ItemTypeIndex"],
            outputCols=["RegionVec", "ItemTypeVec"]
        )
        
        assembler = VectorAssembler(
            inputCols=["RegionVec", "ItemTypeVec", "Order_Month", "Order_Year"],
            outputCol="features"
        )
        
        pipeline = Pipeline(stages=[
            region_indexer,
            item_type_indexer,
            encoder,
            assembler,
            model
        ])
        
        return pipeline

    def _evaluate_model(self, predictions):
        """Оценка модели"""
        evaluator_rmse = RegressionEvaluator(
            labelCol="TotalUnitsSold",
            predictionCol="prediction",
            metricName="rmse"
        )
        
        evaluator_r2 = RegressionEvaluator(
            labelCol="TotalUnitsSold",
            predictionCol="prediction",
            metricName="r2"
        )
        
        rmse = evaluator_rmse.evaluate(predictions)
        r2 = evaluator_r2.evaluate(predictions)
        
        return {
            "rmse": rmse,
            "r2": r2
        }