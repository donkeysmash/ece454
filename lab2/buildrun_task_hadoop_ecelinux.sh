#!/bin/sh

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export HADOOP_HOME=/opt/hadoop-2.7.2
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

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
INPUT=sample_input/myinput.txt
#INPUT=sample_input/smalldata.txt
OUTPUT=output_hadoop_task$1
INTERMEDIATE=intermediate_output

rm -fr $OUTPUT $INTERMEDIATE
$HADOOP_HOME/bin/hadoop jar Task$1.jar Task$1 $INPUT $OUTPUT

cat $OUTPUT/*
