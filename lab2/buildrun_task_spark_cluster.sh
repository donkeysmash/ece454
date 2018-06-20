#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SCALA_HOME=/usr
export CLASSPATH=".:/usr/hdp/2.6.2.38-1/spark2/jars/*"

echo --- Deleting
rm Task$1.jar
rm Task$1*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g Task$1.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task$1.jar Task$1*.class

echo --- Running
INPUT=/tmp/a2_inputs/in2.txt
OUTPUT=/user/${USER}/a2_spark_task$1/

hdfs dfs -rm -R $OUTPUT
#hdfs dfs -copyFromLocal sample_input/smalldata.txt /tmp
time spark-submit --class Task$1 Task$1.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
