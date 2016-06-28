#!/bin/sh
rm -rf bin/*
javac -d bin/ -cp .:log4j-1.2.17.jar:MUtilities.jar:google-api-java-client/libs/google-http-client-1.21.0.jar:json-simple-1.1.1.jar:json-path-0.8.1.jar:src/main/java/ src/main/java/eu/unitn/disi/db/grava/scc/Main.java
java -classpath .:bin/:./MUtilities.jar:./log4j-1.2.17.jar eu/unitn/disi/db/grava/scc/Main 1 0 1 2 10000nodes test stat.txt true
