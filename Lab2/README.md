# Запуск
1. В зависимости от эксперимента нужно выполнить одну из команд:
   
- ``docker-compose -f docker-compose.yml up``
- ``docker-compose -f docker-compose-3d.yml up``
  
2. Перенести данные в контейнер namenode и перенести файлы с кодом в контейнер spark-master
   
``docker cp data/100000_Sales_Records.csv namenode:/``

``docker cp -L src/. spark-master:/opt/bitnami/spark/``


3. Зайти в контейнер namenode и положить данные в hdfs
   
``docker exec -it namenode bash``

``hdfs dfs -put 100000_Sales_Records.csv /``

``exit``

4. В зависимости от эксперимента запустить одну из команд:
   
Если нода 1, то обычная / оптимизированная версия запускаются так:

- ``docker exec -it spark-master spark-submit --master spark://spark-master:7077 main.py -d hdfs://namenode:9000/100000_Sales_Records.csv -n 1 -i 15``
- ``docker exec -it spark-master spark-submit --master spark://spark-master:7077 main.py -d hdfs://namenode:9000/100000_Sales_Records.csv -n 1 -i 15 -o``
  
Если нод 3, то обычная / оптимизированная версия запускаются так:

- ``docker exec -it spark-master spark-submit --master spark://spark-master:7077 main.py -d hdfs://namenode:9000/100000_Sales_Records.csv -n 3 -i 15``
- ``docker exec -it spark-master spark-submit --master spark://spark-master:7077 main.py -d hdfs://namenode:9000/100000_Sales_Records.csv -n 3 -i 15 -o``
5. Сгенерировать график с результатами можно так:
  
``python src/plots.py``
![result_distributions](https://github.com/user-attachments/assets/85f67b17-576b-4804-8686-9dcfeb777486)

# Анализ результатов
Как можно видеть, использование ``.repartition()`` и ``.cache()`` ускоряет обработку данных, однако увеличивает потребление памяти ввиду кэширования. Также заметно, что среднее время выполнения операция становится меньше при использовании 3 нод в сравнении с 1 нодой.
