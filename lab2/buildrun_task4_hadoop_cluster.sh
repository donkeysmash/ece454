#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export CLASSPATH=`hadoop classpath`

echo --- Deleting
rm Task4.jar
rm Task4*.class

echo --- Compiling
$JAVA_HOME/bin/javac Task4.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task4.jar Task4*.class

echo --- Running
INPUT=/tmp/a2_inputs/in2.txt
OUTPUT=/user/${USER}/a2_hadoop_task4/
INTERMEDIATE=/user/${USER}/intermediate_output

hdfs dfs -rm -R $OUTPUT $INTERMEDIATE
#hdfs dfs -copyFromLocal sample_input/smalldata.txt /tmp
time hadoop jar Task4.jar Task4 -D mapreduce.map.java.opts=-Xmx4g -D mapred.max.split.size=16777216 $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
