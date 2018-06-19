#!/bin/bash

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export SCALA_HOME=/opt/scala-2.11.6
export SPARK_HOME=/opt/spark-2.1.0-bin-hadoop2.7
export CLASSPATH=.:"$SPARK_HOME/jars/*"

echo --- Deleting
rm Task$1.jar
rm Task$1*.class

echo --- Compiling
$SCALA_HOME/bin/scalac Task$1.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task$1.jar Task$1*.class

echo --- Running
INPUT=sample_input/smalldata.txt
OUTPUT=output_spark_task$1

rm -fr $OUTPUT
$SPARK_HOME/bin/spark-submit --master "local[*]" --class Task$1 Task$1.jar $INPUT $OUTPUT

cat $OUTPUT/*
