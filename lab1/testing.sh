#!/bin/sh

#
# Wojciech Golab, 2016
#

JAVA=/usr/lib/jvm/java-1.8.0/bin/java
JAVA_CC=/usr/lib/jvm/java-1.8.0/bin/javac
THRIFT_CC=/opt/bin/thrift

#$JAVA -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" FENode 10424
#$JAVA -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" BENode ecelinux5 10424 10424
#$JAVA -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" Client ecelinux5 10424 hello
$JAVA_CC gen-java/*.java -cp .:"lib/*"
$JAVA_CC *.java -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*"

#$JAVA -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" BENode ecelinux5 10765 10765
$JAVA -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" Client ecelinux5 10424 aaaaa



# /usr/lib/jvm/java-1.8.0/bin/java -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*"
