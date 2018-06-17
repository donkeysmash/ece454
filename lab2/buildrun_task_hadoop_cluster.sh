#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export CLASSPATH=`hadoop classpath`

echo --- Deleting
rm Task$1.jar
rm Task$1*.class

echo --- Compiling
$JAVA_HOME/bin/javac Task$1.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task$1.jar Task$1*.class

echo --- Running
INPUT=/tmp/a2_inputs/in2.txt
OUTPUT=/user/${USER}/a2_hadoop_task$1/

hdfs dfs -rm -R $OUTPUT
#hdfs dfs -copyFromLocal sample_input/smalldata.txt /tmp
time hadoop jar Task$1.jar Task$1 $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
