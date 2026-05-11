#!/bin/bash

BASE_DIR=$(cd "$(dirname "$0")"; pwd)

export PYSPARK_PYTHON=/usr/bin/python3.8
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.8
export PYTHONPATH=$BASE_DIR/driver_env
# Silence deprecation warnings for cleaner logs
export PYTHONWARNINGS="ignore:Python 3.8 is no longer supported"

ICEBERG_JAR="/opt/cloudera/parcels/SPARK3-3.3.2.3.3.7190.0-91-1.p0.45265883/lib/spark3/iceberg/iceberg-spark-runtime-3.3_2.12-1.3.0.3.3.7190.0-91.jar"

echo "🚀 Starting Spark job from $BASE_DIR..."

spark3-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 4 \
  --executor-memory 8G \
  --jars $ICEBERG_JAR \
  --archives $BASE_DIR/executor_env.zip#my_env \
  --conf "spark.executorEnv.PYTHONPATH=my_env" \
  --conf "spark.yarn.appMasterEnv.PYTHONPATH=my_env" \
  --conf "spark.sql.execution.arrow.pyspark.enabled=true" \
$BASE_DIR/enterprise_pii_scan.py 2>/dev/null
