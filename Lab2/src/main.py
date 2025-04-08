import time
from argparse import ArgumentParser
from pyspark.sql import SparkSession
from tqdm import tqdm
import pandas as pd
import os

from data_pipeline import apply_transformations


def profile_spark_memory(sc):
    spark_memory = sc._jsc.sc().getExecutorMemoryStatus()
    spark_memory_dict = sc._jvm.scala.collection.JavaConverters.mapAsJavaMapConverter(spark_memory).asJava()
    reserved_memory = 0
    for values in spark_memory_dict.values():
        reserved_memory += (values._1() / 1024**2 - values._2() / 1024**2)
    return reserved_memory


def main(data_path: str, num_nodes: int, optimized: bool, iters: int) -> None:
    spark: SparkSession = (
        SparkSession.builder
        .appName('SparkApp')
        .getOrCreate()
    )

    utilized_time = []
    utilized_RAM = []

    for _ in tqdm(range(iters)):
        dataset = spark.read.option("inferSchema", True).csv(data_path, header=True)

        start_time = time.time()        
        apply_transformations(dataset, optimized)
        end_time = time.time()

        utilized_time.append(end_time - start_time)
        utilized_RAM.append(profile_spark_memory(spark.sparkContext))

    file_name = f"optimized_{num_nodes}_nodes.csv" if optimized else f"not_optimized_{num_nodes}_nodes.csv"
    pd.DataFrame({"Time": utilized_time, "RAM": utilized_RAM}).to_csv(file_name)
    spark.stop()

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--data-path', '-d')
    parser.add_argument('--num-nodes', '-n', default = '1')
    parser.add_argument('--optimized', '-o', action='store_true')
    parser.add_argument('--iters', '-i', default = 100, type=int)
    args = parser.parse_args()

    main(args.data_path, args.num_nodes, args.optimized, args.iters)
