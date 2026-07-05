FROM apache/spark:4.0.0

WORKDIR /app

RUN mkdir -p /tmp/.ivy2/cache && chmod -R 777 /tmp/.ivy2

# Switch to root to install Python dependencies
USER root
RUN pip install --no-cache-dir pandas pyarrow clickhouse-driver

COPY processor.py ./

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--conf", "spark.driver.extraJavaOptions=-Duser.home=/tmp", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0", \
     "processor.py"]