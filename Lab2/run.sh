docker cp data/100000_Sales_Records.csv namenode:/
docker cp src/. spark-master:/opt/bitnami/spark/
docker exec namenode hdfs dfs -put 100000_Sales_Records.csv /

nodes_num="1"
iters_num="15"
opt="false"

while [ -n "$1" ]
do
case "$1" in
-n) nodes_num="$2";;
-i) iters_num="$2";;
-o) opt="true";;
esac
shift
done

echo $opt

if [ "$opt" == "true" ]; then
    echo "Running optimized version with $nodes_num nodes and $iters_num iters"
    docker exec -it spark-master spark-submit --master spark://spark-master:7077 main.py -d hdfs://namenode:9000/100000_Sales_Records.csv -n $nodes_num -i $iters_num -o
else
    echo "Running base version with $nodes_num nodes and $iters_num iters"
    docker exec -it spark-master spark-submit --master spark://spark-master:7077 main.py -d hdfs://namenode:9000/100000_Sales_Records.csv -n $nodes_num -i $iters_num
fi