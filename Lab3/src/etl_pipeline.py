import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, to_date, when
from pyspark.sql.types import DateType
from delta.tables import DeltaTable
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.init_data_path = "/app/data/100000_Sales_Records.csv"
        self.bronze_path = "/app/data/bronze/sales_data"
        self.silver_path = "/app/data/silver/sales_data"
        self.gold_path = "/app/data/gold/sales_agg"

    def run_pipeline(self):
        self._ingest_to_bronze()
        self._process_to_silver()
        self._aggregate_to_gold()

    def _ingest_to_bronze(self):
        """Загрузка данных в bronze слой"""
        try:            
            df = self.spark.read.option("inferSchema", True).csv(self.init_data_path, header=True)
            df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])

            (df.repartition(7, "Region") # 7 unique values in Region
               .write
               .format("delta")
               .mode("overwrite")
               .save(self.bronze_path))
            
            logger.info("Data successfully ingested to bronze layer")
            
        except Exception as e:
            logger.error(f"Error in bronze layer ingestion: {str(e)}", exc_info=True)
            raise

    def _process_to_silver(self):
        """Обработка данных и сохранение в silver слой"""
        try:
            df = self.spark.read.format("delta").load(self.bronze_path)
            df_clean = self._clean_data(df)
            df_transformed = (df_clean
                .withColumn("Order_Date", to_date(col("Order_Date"), "M/d/yyyy"))
                .withColumn("Ship_Date", to_date(col("Ship_Date"), "M/d/yyyy"))
                .withColumn("Order_Year", year(col("Order_Date")))
                .withColumn("Order_Month", month(col("Order_Date")))
                .withColumn("Order_Day", dayofmonth(col("Order_Date")))
            )
            (df_transformed.repartition(15)
               .write
               .format("delta")
               .mode("overwrite")
               .save(self.silver_path))
            
            delta_table = DeltaTable.forPath(self.spark, self.silver_path)
            delta_table.optimize().executeCompaction()
            
            logger.info("Data successfully processed to silver layer")
            
        except Exception as e:
            logger.error(f"Error in silver layer processing: {str(e)}", exc_info=True)
            raise

    def _clean_data(self, df):
        df = df.dropDuplicates(["Order_ID"])
        df = df.na.drop("any")
        df = df.withColumn("Total_Revenue_Valid", 
                          when(col("Total_Revenue") == col("Units_Sold") * col("Unit_Price"), True)
                          .otherwise(False))
        df = df.filter(col("Total_Revenue_Valid") == True).drop("Total_Revenue_Valid")
        
        return df

    def _aggregate_to_gold(self):
        try:
            df = self.spark.read.format("delta").load(self.silver_path)
            
            df_agg = (df.groupBy("Region", "Item_Type", "Order_Year", "Order_Month")
                     .agg({"Units_Sold": "sum",
                           "Total_Revenue": "sum",
                           "Total_Cost": "sum",
                           "Total_Profit": "sum"})
                     .withColumnRenamed("sum(Units_Sold)", "TotalUnitsSold")
                     .withColumnRenamed("sum(Total_Revenue)", "TotalRevenue")
                     .withColumnRenamed("sum(Total_Cost)", "TotalCost")
                     .withColumnRenamed("sum(Total_Profit)", "TotalProfit"))
            
            (df_agg.repartition(7*12, "Region", "Order_Year")  
               .write
               .format("delta")
               .mode("overwrite")
               .partitionBy("Region", "Order_Year") 
               .save(self.gold_path))
            
            delta_table = DeltaTable.forPath(self.spark, self.gold_path)
            delta_table.optimize().executeCompaction()
            delta_table.vacuum(168)
            
            logger.info("Data successfully aggregated to gold layer")
            
        except Exception as e:
            logger.error(f"Error in gold layer aggregation: {str(e)}", exc_info=True)
            raise