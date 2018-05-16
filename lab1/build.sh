#!/bin/sh

#
# Wojciech Golab, 2016
#

JAVA=/usr/lib/jvm/java-1.8.0/bin/java
JAVA_CC=/usr/lib/jvm/java-1.8.0/bin/javac
THRIFT_CC=/opt/bin/thrift

echo --- Cleaning
rm -f *.jar
rm -f *.class
rm -fr gen-java

echo --- Compiling Thrift IDL
$THRIFT_CC --version
$THRIFT_CC --gen java a1.thrift

echo --- Compiling Java
$JAVA_CC -version
$JAVA_CC gen-java/*.java -cp .:"lib/*"
$JAVA_CC *.java -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*"

echo --- Done, now run your code.
# $JAVA -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" FENode 10123
# $JAVA -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" BENode localhost 10123 10124
# $JAVA -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" Client localhost 10123 hello
