#!/bin/sh

sbt package && \
$SPARK_HOME/bin/spark-submit \
    --master "local[2]" \
    --class net.xton.EnronReport \
    --jars "`sbt 'export runtime:fullClasspath' | tr : ,`" \
    target/scala-2.11/spark-reademails_2.11-0.1.jar "$@"
