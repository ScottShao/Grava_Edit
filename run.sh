#!/bin/sh
../jdk1.8.0_101/bin/java -Xmx90000M -Xms90000M -Xss10M -classpath .:bin/:./MUtilities.jar:./log4j-1.2.17.jar eu/unitn/disi/db/grava/scc/Main 1 1 1 2 100000nodes  queryFolder/100000nodes stat.txt true
