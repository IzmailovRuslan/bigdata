
FROM bitnami/spark:latest

USER root
RUN apt-get -y update --fix-missing
RUN apt-get -y upgrade --fix-missing
RUN apt-get install -y python3-pip --fix-missing
RUN pip3 install pandas psutil tqdm

USER 1001

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]
