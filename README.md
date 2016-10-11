## DistributedERTool
#Overview

#Installation

1.Install the limes-core.jar to the local maven repository

`mvn install:install-file -Dfile={path/to/limes-core.jar} -DgroupId=org.aksw.limes.core -DartifactId=limes-core -Dversion=1.1.0-SNAPSHOT -Dpackaging=jar`

2.Build the spark application 

`mvn clean package` 

The SparkApplication-0.0.1-SNAPSHOT.jar will be created

#Execution

Execute the application against a spark cluster using the following command

``````
./bin/spark-submit \
  --class spark.Controller \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  SparkApplication-0.0.1-SNAPSHOT.jar {/path/to/limes_configuration_file.xml} {/path/to/limes.dtd} {purging_enabled=(true,false)}


``````
