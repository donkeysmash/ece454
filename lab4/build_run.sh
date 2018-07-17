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

echo --- Cleaning
rm -f A4*.class

echo --- Compiling Java
$JAVA_CC A4*.java
if [ $? -eq 0 ]
then
  echo "Success..."
else
  echo "Error..."
  exit 1
fi

echo --- Running

$JAVA A4Application $KBROKERS $APP_NAME $STOPIC $CTOPIC $OTOPIC $STATE_STORE_DIR
