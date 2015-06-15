#!/bin/bash

# Usage
# SPARK_DRIVER_MEMORY=48g $SPARK_HOME/bin/spark-submit --executor-memory 48g \
#     --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 280 \
#     --master spark://localhost:7077 \
#     --driver-java-options "-XX:+PrintFlagsFinal" \
#     ${BWAMEM_HOME}/target/cloud-scale-bwamem-0.1.0-assembly.jar help

if [[ $# != 1 ]]; then
    echo usage: pair-end_csbwamem_flow.sh upload\|kernel
    exit 1
fi

TASK=$1

if [[ $TASK != upload && $TASK != kernel ]]; then
    echo You must specify upload or kernel
    exit 1
fi

N_NODES=$(cat ${HADOOP_HOME}/etc/hadoop/slaves | wc -l)
CORES_PER_NODE=$(cat /proc/cpuinfo | grep processor | wc -l)
TOTAL_CORES=$(echo $N_NODES \* $CORES_PER_NODE | bc)

if [[ $TASK == upload ]]; then
    # store RDD
    # pair-end
    echo "Starting upload"
    SPARK_DRIVER_MEMORY=40g $SPARK_HOME/bin/spark-submit --executor-memory 40g \
        --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores $TOTAL_CORES \
        --master spark://$(hostname):7077 \
        --driver-java-options "-XX:+PrintFlagsFinal" \
        --conf spark.akka.frameSize=30 \
        ${BWAMEM_HOME}/target/cloud-scale-bwamem-0.1.0-assembly.jar upload-fastq \
        -bn 1000000 1 $TOTAL_CORES /scratch/jmg3/HCC1954_1_10M.fq \
        /scratch/jmg3/HCC1954_2_10M.fq hdfs://$(hostname):54310/HCC1954_10Mreads.fq
    echo "Done with upload"
fi

if [[ $TASK == kernel ]]; then
    # run cloud-scale bwamem
    # SAM output
    # When running with 10M reads, the last parameter (which indicates the
    # number of input files to read) must be set to 20. When doing 100M reads, I
    # believe it is supposed to be set to 80?
    echo "Starting kernel"
    SPARK_DRIVER_MEMORY=40g $SPARK_HOME/bin/spark-submit --executor-memory 40g \
        --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores $TOTAL_CORES \
        --master spark://$(hostname):7077 \
        --driver-java-options "-XX:+PrintFlagsFinal" \
        --conf spark.driver.maxResultSize=40g \
        ${BWAMEM_HOME}/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem \
        -bfn 20 -bPSW 1 -sbatch 5 -bPSWJNI 1 \
        -jniPath ${BWAMEM_HOME}/target/jniNative.so -oChoice 1 \
        -oPath /scratch/jmg3/HCC1954_10Mreads.sam \
        1 \
        /scratch/jmg3/ReferenceMetadata/human_g1k_v37.fasta \
        hdfs://$(hostname):54310/HCC1954_10Mreads.fq \
        20
    echo "Done with kernel"
fi

# ADAM output
#SPARK_DRIVER_MEMORY=24g /home/pengwei/spark-1.1.0/bin/spark-submit --executor-memory 36g --class cs.ucla.edu.bwaspark.BWAMEMSpark --total-executor-cores 48 --master spark://Jc11:7077 --driver-java-options "-XX:+PrintFlagsFinal" /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/cloud-scale-bwamem-0.1.0-assembly.jar cs-bwamem -bfn 1 -bPSW 1 -sbatch 10 -bPSWJNI 1 -jniPath /home/ytchen/incubator/cloud-scale-bwamem-0.1.0/target/jniNative.so -oChoice 2 -oPath hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/output/test_reads.adam 1 /home/hadoopmaster/genomics/ReferenceMetadata/human_g1k_v37.fasta hdfs://Jc11:9000/user/ytchen/data/correctness_verification/pair-end/test_reads.fq 1

