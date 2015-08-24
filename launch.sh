#!/bin/bash
# pass the classname which shall be executed
# --conf spark.storage.memoryFraction=0.3
/homeLocal/god/opt/spark-1.3.1-bin-hadoop2.6/bin/spark-submit --executor-memory 6G --driver-memory 4G --class de.unihamburg.vsis.sddf.exe.$1 --master spark://vsispool20.informatik.uni-hamburg.de:7077 /homeLocal/god/sddf/code/projects/sddf/target/scala-2.10/sddf-assembly-0.1.0.jar
