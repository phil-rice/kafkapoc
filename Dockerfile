FROM flink:2.0.0-scala_2.12-java21

COPY conf/config.yaml /opt/flink/conf/config.yaml

EXPOSE 8088
EXPOSE 9400

ENTRYPOINT ["java",
  "-cp", "/opt/flink/lib/*:/opt/flink/usrlib/*",
  "com.hcltech.rmg.performance.FlinkWorker"
]
