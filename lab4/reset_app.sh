#!/bin/sh

#
# Wojciech Golab, 2017
#

source ./settings.sh

unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
JAVA=$JAVA_HOME/bin/java
JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=.:"${KAFKA_HOME}/libs/*"

echo --- Compiling Java
$JAVA_CC Reset*.java
if [ $? -eq 0 ]
then
  echo "Success..."
else
  echo "Error..."
  exit 1
fi

echo --- Local reset
$JAVA ResetApplication $KBROKERS $APP_NAME student-topic-$USER classroom-topic-$USER output-topic-$USER $STATE_STORE_DIR

echo --- Global reset
$KAFKA_HOME/bin/kafka-streams-application-reset.sh --application-id $APP_NAME --bootstrap-servers $KBROKERS --zookeeper $ZKSTRING --input-topics $STOPIC,$CTOPIC
