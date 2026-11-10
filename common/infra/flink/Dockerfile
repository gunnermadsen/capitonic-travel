FROM apache/flink:2.2.1-java17@sha256:87c436d143ddeafbe977dd299ee63cef574b57688dc29f13cad90e0c7e2f6126
USER root
RUN set -eu; \
    curl -fSL --retry 3 https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar -o /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar; \
    echo '5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616  /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar' | sha256sum -c -; \
    curl -fSL --retry 3 https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.2/1.12.0/iceberg-flink-runtime-2.2-1.12.0.jar -o /opt/flink/lib/iceberg-flink-runtime-2.2-1.12.0.jar; \
    echo '72c051af1fc6adf86d7ece8a81ce4f3ffa5016690c52fd2f653419c6f398177d51b29e724785452fbd8af3f807b8d00ac2334532a86ab917ab6176488ca25735  /opt/flink/lib/iceberg-flink-runtime-2.2-1.12.0.jar' | sha512sum -c -; \
    curl -fSL --retry 3 https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar -o /opt/flink/lib/iceberg-aws-bundle-1.12.0.jar; \
    echo 'b01456f80142c8837004e31329a0f40db8a9d44917a4a694cb7d8d6d3a68cb20c61e6f7788fa1bdcb95c0edf724322b49667d6f9ef60fc9cc46da5c24ce5c7de  /opt/flink/lib/iceberg-aws-bundle-1.12.0.jar' | sha512sum -c -; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop /opt/flink/work; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-2.2.1.jar /opt/flink/plugins/s3-fs-hadoop/; \
    chown flink:flink /opt/flink/work
# Iceberg's catalog factory needs Hadoop classes on the application classpath.
# The S3 filesystem plugin above remains isolated in plugins/.
RUN set -eu; \
    curl -fSL --retry 3 https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.2/hadoop-client-api-3.4.2.jar -o /opt/flink/lib/hadoop-client-api-3.4.2.jar; \
    echo '607c0299f4df33013bd0a0f37c8cf255c635bbaddd247b8fec68c210cc17d19f  /opt/flink/lib/hadoop-client-api-3.4.2.jar' | sha256sum -c -; \
    curl -fSL --retry 3 https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.2/hadoop-client-runtime-3.4.2.jar -o /opt/flink/lib/hadoop-client-runtime-3.4.2.jar; \
    echo '18d071c7f72f8a946677b398a57af9c7afa2dd41a81023c310f99373fec200b3  /opt/flink/lib/hadoop-client-runtime-3.4.2.jar' | sha256sum -c -
USER flink
