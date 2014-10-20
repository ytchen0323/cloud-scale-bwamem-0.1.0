##./bin/spark-submit --class cs.ucla.edu.bwaspark.BWAMEMSpark --master local[24] /home/hadoopmaster/standard_projects/bwa-spark-0.3.1/target/bwa-spark-0.3.1-assembly.jar

# run cloud-scale bwamem
SPARK_DRIVER_MEMORY=24g /home/pengwei/spark-1.0.1_modified/bin/spark-submit --executor-memory 36g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem -bfn 4 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so -oSAM 1 -oSAMPath HCC1954_1_100M_reads_7.5Mpart.sam 0 /home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_100M_reads_7.5Mpart.fq 54

# store RDD
# single-end
#SPARK_DRIVER_MEMORY=40g /home/pengwei/spark-1.0.1_modified/bin/spark-submit --executor-memory 20g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar upload-fastq -bn 7500000 0 48 /home/ytchen/genomics/data/correctness_verification/HCC1954_1_100M_reads.fq hdfs://Jc11:9000/user/ytchen/data/correctness_verification/single-end/HCC1954_1_100M_reads_7.5Mpart.fq
# pair-end
#SPARK_DRIVER_MEMORY=40g /home/pengwei/spark-1.0.1_modified/bin/spark-submit --executor-memory 20g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar upload-fastq -bn 1000000 1 48 /home/ytchen/genomics/data/correctness_verification/HCC1954_1_1-1M.fq /home/ytchen/genomics/data/correctness_verification/HCC1954_2_1-1M.fq hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/HCC1954_1-1M.fq

# test command line
#SPARK_DRIVER_MEMORY=24g /home/pengwei/spark-1.0.1_modified/bin/spark-submit --executor-memory 36g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar help
