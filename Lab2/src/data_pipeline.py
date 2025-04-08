import pyspark
from pyspark.sql.dataframe import DataFrame as spDataFrame
from pyspark.sql.functions import col, count, mean, stddev, year, month, when, corr, percentile_approx, quarter, datediff, rank, to_timestamp, col, udf
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from datetime import datetime


def apply_transformations(df: spDataFrame, optimized: bool):
    # Этап 1: Предварительная обработка и обогащение данных
    # 1.1 Преобразование дат и извлечение временных признаков
    func =  udf(lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())

    df = df.withColumn("Order Date", func(col("Order Date")))
    df = df.withColumn("Ship Date", func(col("Ship Date")))
    #df = df.withColumn("order_year", pyspark.sql.functions.year(col("Order Date")))
    
    df = df.withColumn("order_year", pyspark.sql.functions.year("Order Date"))
    df = df.withColumn("order_month", month("Order Date"))
    df = df.withColumn("order_quarter", quarter("Order Date"))
    df = df.withColumn("processing_days", datediff("Ship Date", "Order Date"))
    
    # 1.2 Добавление аналитических признаков
    df = df.withColumn("profit_margin", col("Total Profit") / col("Total Revenue"))
    df = df.withColumn("unit_profit", col("Unit Price") - col("Unit Cost"))  
    
    if optimized:
        # Оптимальное партиционирование для последующих операций
        df = df.repartition(8, "Region", "Order Date")
        df = df.cache()

    
    # Этап 2: Комплексная аналитика продаж
    # 2.1 Анализ временных трендов
    sales_trends = df.groupBy("order_year", "order_quarter", "order_month").agg(
        count("*").alias("order_count"),
        pyspark.sql.functions.sum("Units Sold").alias("total_units"),
        pyspark.sql.functions.sum("Total Revenue").alias("total_revenue"),
        pyspark.sql.functions.sum("Total Profit").alias("total_profit"),
        mean("profit_margin").alias("avg_margin"),
        mean("processing_days").alias("avg_processing_days")
    ).orderBy("order_year", "order_quarter", "order_month")
    
    # 2.2 Анализ по регионам и странам
    geo_analysis = df.groupBy("Region", "Country").agg(
        count("*").alias("order_count"),
        pyspark.sql.functions.sum("Total Revenue").alias("total_revenue"),
        pyspark.sql.functions.sum("Total Profit").alias("total_profit"),
        mean("profit_margin").alias("avg_margin"),
        percentile_approx("Total Profit", [0.25, 0.5, 0.75]).alias("profit_percentiles")
    ).orderBy("Region", col("total_profit").desc())
    
    # 2.3 Анализ по типам товаров и каналам продаж
    product_analysis = df.groupBy("Item Type", "Sales Channel").agg(
        count("*").alias("order_count"),
        pyspark.sql.functions.sum("Units Sold").alias("total_units"),
        mean("Unit Price").alias("avg_price"),
        mean("Unit Cost").alias("avg_cost"),
        mean("unit_profit").alias("avg_unit_profit"),
        pyspark.sql.functions.sum("Total Profit").alias("total_profit")
    ).orderBy("Item Type", "Sales Channel")
    
    # 2.4 Корреляционный анализ
    corr_matrix = df.select(
        corr("Units Sold", "Total Profit").alias("units_profit_corr"),
        corr("Unit Price", "Total Revenue").alias("price_revenue_corr"),
        corr("processing_days", "profit_margin").alias("days_margin_corr")
    ).collect()[0]
    
 
    
    # Этап 3: Многократное использование данных для разных аналитических задач    
    # 3.1 Анализ сезонности (повторное использование обогащенного DF)
    for year in [2014, 2015, 2016]:
        yearly_analysis = df.filter(col("order_year") == year).groupBy("order_month").agg(
            pyspark.sql.functions.sum("Total Profit").alias("monthly_profit"),
            count("*").alias("order_count")
        ).orderBy("order_month")
        
        if year == 2015:
            first_year_results = yearly_analysis.collect()

    # 3.2 Выявление топовых товаров по регионам (сложные агрегации)
    product_by_region = df.groupBy("Region", "Item Type").agg(
        pyspark.sql.functions.sum("Total Profit").alias("total_profit"),
        count("*").alias("order_count")
    )

    window_spec = Window.partitionBy(col("Region")).orderBy(col("total_profit").desc())

    top_products_by_region = product_by_region.withColumn("rank", rank().over(window_spec)) \
                                        .filter(col("rank") <= 3) \
                                        .select(
                                            col("Region"),
                                            col("Item Type"),
                                            col("total_profit"),
                                            col("rank")
                                        )
    
    # 3.3 Анализ эффективности доставки
    shipping_efficiency = df.groupBy("Region").agg(
        mean("processing_days").alias("avg_processing_days"),
        stddev("processing_days").alias("stddev_processing_days"),
        count("*").alias("order_count")
    ).orderBy("Region")
