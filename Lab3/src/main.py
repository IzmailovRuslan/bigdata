import logging
from pyspark.sql import SparkSession
from etl_pipeline import ETLPipeline
from ml_model import SalesPredictor
from delta import configure_spark_with_delta_pip
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='/app/logs/app.log'
)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Spark сессия с Delta Lake"""
    builder = SparkSession.builder \
        .appName("SalesAnalysisPipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("INFO")
        logger.info("Spark session initialized successfully")

        etl = ETLPipeline(spark)
        etl.run_pipeline()

        ml = SalesPredictor(spark)
        ml.train_and_evaluate()

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()