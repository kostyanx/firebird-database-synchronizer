@echo on

java -Xmx512m -Dlog4j.configurationFile="log4j2.xml" -jar catcab-database-synchronizer-1.0.0.jar %*
