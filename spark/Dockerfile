FROM apache/spark:3.5.1

USER root
RUN pip3 install --no-cache-dir pandas
RUN pip3 install --no-cache-dir --no-deps delta-spark==3.2.0 
# no-deps so it doesn't also pull spark
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER spark