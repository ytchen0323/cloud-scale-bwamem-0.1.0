##./bin/spark-submit --class cs.ucla.edu.bwaspark.BWAMEMSpark --master local[24] /home/hadoopmaster/standard_projects/bwa-spark-0.3.1/target/bwa-spark-0.3.1-assembly.jar
##SPARK_DRIVER_MEMORY=12g ./bin/spark-submit --executor-memory 20g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 36 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/pengwei/standard_projects/bwa-spark-0.3.1/target/bwa-spark-0.3.1-assembly.jar
SPARK_DRIVER_MEMORY=18g /home/pengwei/spark-1.0.1_modified/bin/spark-submit --executor-memory 44g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 72 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/pengwei/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar
##SPARK_DRIVER_MEMORY=18g /home/pengwei/spark-1.0.1_modified/bin/spark-submit --executor-memory 44g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 72 --master local[24] --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar
##SPARK_DRIVER_MEMORY=28g /home/pengwei/spark-1.0.1_modified/bin/spark-submit --executor-memory 32g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 72 --master local[24] --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar
