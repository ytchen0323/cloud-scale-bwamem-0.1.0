/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package cs.ucla.edu.bwaspark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.worker1.BWAMemWorker1._
import cs.ucla.edu.bwaspark.worker1.BWAMemWorker1Batched._
import cs.ucla.edu.bwaspark.worker2.BWAMemWorker2._
import cs.ucla.edu.bwaspark.worker2.MemSamPe._
import cs.ucla.edu.bwaspark.sam.SAMHeader
import cs.ucla.edu.bwaspark.sam.SAMWriter
import cs.ucla.edu.bwaspark.sam.SAMHDFSWriter
import cs.ucla.edu.bwaspark.debug.DebugFlag._
import cs.ucla.edu.bwaspark.fastq._
import cs.ucla.edu.bwaspark.util.SWUtil._
import cs.ucla.edu.avro.fastq._
import cs.ucla.edu.bwaspark.commandline._
import cs.ucla.edu.bwaspark.broadcast.ReferenceBroadcast

import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.models.{SequenceDictionary, RecordGroup, RecordGroupDictionary}

import htsjdk.samtools.SAMFileHeader

import java.io.FileReader
import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI

object FastMap {
  private val MEM_F_PE: Int = 0x2
  private val MEM_F_ALL = 0x8
  private val MEM_F_NO_MULTI = 0x10
  private val packageVersion = "cloud-scale-bwamem-0.2.2"
  private val NO_OUT_FILE = 0
  private val SAM_OUT_LOCAL = 1
  private val ADAM_OUT = 2
  private val SAM_OUT_DFS = 3

  /**
    *  memMain: the main function to perform read mapping
    *
    *  @param sc the spark context object
    *  @param bwamemArgs the arguments of CS-BWAMEM
    */
  def memMain(sc: SparkContext, bwamemArgs: BWAMEMCommand) 
  {
    val fastaLocalInputPath = bwamemArgs.fastaInputPath        // the local BWA index files (bns, pac, and so on)
    val fastqHDFSInputPath = bwamemArgs.fastqHDFSInputPath     // the raw read file stored in HDFS
    val isPairEnd = bwamemArgs.isPairEnd                       // perform pair-end or single-end mapping
    val batchFolderNum = bwamemArgs.batchedFolderNum           // the number of raw read folders in a batch to be processed
    val isPSWBatched = bwamemArgs.isPSWBatched                 // whether the pair-end Smith Waterman is performed in a batched way
    val subBatchSize = bwamemArgs.subBatchSize                 // the number of reads to be processed in a subbatch
    val isPSWJNI = bwamemArgs.isPSWJNI                         // whether the native JNI library is called for better performance
    val jniLibPath = bwamemArgs.jniLibPath                     // the JNI library path in the local machine
    val outputChoice = bwamemArgs.outputChoice                 // the output format choice
    val outputPath = bwamemArgs.outputPath                     // the output path in the local or distributed file system
    val readGroupString = bwamemArgs.headerLine                // complete read group header line: Example: @RG\tID:foo\tSM:bar

    val samHeader = new SAMHeader
    var adamHeader = new SequenceDictionary
    val samFileHeader = new SAMFileHeader
    var seqDict: SequenceDictionary = null
    var readGroupDict: RecordGroupDictionary = null
    var readGroup: RecordGroup = null

    // get HDFS information
    val conf: Configuration = new Configuration
    val hdfs: FileSystem = FileSystem.get(new URI(fastqHDFSInputPath), conf)
    val status = hdfs.listStatus(new Path(fastqHDFSInputPath))
    val fastqInputFolderNum = status.size                      // the number of folders generated in the HDFS for the raw reads
    bwamemArgs.fastqInputFolderNum = fastqInputFolderNum       // the number of folders generated in the HDFS for the raw reads
    println("HDFS master: " + hdfs.getUri.toString)
    println("Input HDFS folder number: " + bwamemArgs.fastqInputFolderNum)

    if(samHeader.bwaSetReadGroup(readGroupString)) {
      println("Head line: " + samHeader.readGroupLine)
      println("Read Group ID: " + samHeader.bwaReadGroupID)
    }
    else println("Error on reading header")
    val readGroupName = samHeader.bwaReadGroupID

    // loading index files
    println("Load Index Files")
    val bwaIdx = new BWAIdxType
    bwaIdx.load(fastaLocalInputPath, 0)

    // loading BWA MEM options
    println("Load BWA-MEM options")
    val bwaMemOpt = new MemOptType
    bwaMemOpt.load

    bwaMemOpt.flag |= MEM_F_ALL
    bwaMemOpt.flag |= MEM_F_NO_MULTI
    
    // write SAM header
    println("Output choice: " + outputChoice)
    if(outputChoice == ADAM_OUT) {
      samHeader.bwaGenSAMHeader(bwaIdx.bns, packageVersion, readGroupString, samFileHeader)
      seqDict = SequenceDictionary(samFileHeader)
      readGroupDict = RecordGroupDictionary.fromSAMHeader(samFileHeader)
      readGroup = readGroupDict(readGroupName)
    }

    // pair-end read mapping
    if(isPairEnd) {
      bwaMemOpt.flag |= MEM_F_PE
      if(outputChoice == SAM_OUT_LOCAL || outputChoice == SAM_OUT_DFS)
        memPairEndMapping(sc, bwamemArgs, bwaMemOpt, bwaIdx, samHeader)
      else if(outputChoice == ADAM_OUT)
        memPairEndMapping(sc, bwamemArgs, bwaMemOpt, bwaIdx, samHeader, seqDict, readGroup)       
    }
    // single-end read mapping
    else {
      if(outputChoice == SAM_OUT_LOCAL || outputChoice == SAM_OUT_DFS)
        memSingleEndMapping(sc, fastaLocalInputPath, fastqHDFSInputPath, fastqInputFolderNum, batchFolderNum, bwaMemOpt, bwaIdx, outputChoice, outputPath, samHeader)
      else if(outputChoice == ADAM_OUT)
        memSingleEndMapping(sc, fastaLocalInputPath, fastqHDFSInputPath, fastqInputFolderNum, batchFolderNum, bwaMemOpt, bwaIdx, outputChoice, outputPath, samHeader, seqDict, readGroup)
    }

  } 


  /**
    *  memPairEndMapping: the main function to perform pair-end read mapping
    *
    *  @param sc the spark context object
    *  @param bwamemArgs the arguments of CS-BWAMEM
    *  @param bwaMemOpt the MemOptType object
    *  @param bwaIdx the BWAIdxType object
    *  @param samHeader the SAM header file used for writing SAM output file
    *  @param seqDict (optional) the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup (optional) the read group: used for ADAM format output
    */
  private def memPairEndMapping(sc: SparkContext, bwamemArgs: BWAMEMCommand, bwaMemOpt: MemOptType, bwaIdx: BWAIdxType, 
                                samHeader: SAMHeader, seqDict: SequenceDictionary = null, readGroup: RecordGroup = null) 
  {
    // Get the input arguments
    val fastaLocalInputPath = bwamemArgs.fastaInputPath        // the local BWA index files (bns, pac, and so on)
    val fastqHDFSInputPath = bwamemArgs.fastqHDFSInputPath     // the raw read file stored in HDFS
    val fastqInputFolderNum = bwamemArgs.fastqInputFolderNum   // the number of folders generated in the HDFS for the raw reads
    val batchFolderNum = bwamemArgs.batchedFolderNum           // the number of raw read folders in a batch to be processed
    val isPSWBatched = bwamemArgs.isPSWBatched                 // whether the pair-end Smith Waterman is performed in a batched way
    val subBatchSize = bwamemArgs.subBatchSize                 // the number of reads to be processed in a subbatch
    val isPSWJNI = bwamemArgs.isPSWJNI                         // whether the native JNI library is called for better performance
    val jniLibPath = bwamemArgs.jniLibPath                     // the JNI library path in the local machine
    val outputChoice = bwamemArgs.outputChoice                 // the output format choice
    val outputPath = bwamemArgs.outputPath                     // the output path in the local or distributed file system
    val isSWExtBatched = bwamemArgs.isSWExtBatched             // whether the SWExtend is executed in a batched way
    val swExtBatchSize = bwamemArgs.swExtBatchSize             // the batch size used for used for SWExtend
    val isFPGAAccSWExtend = bwamemArgs.isFPGAAccSWExtend       // whether the FPGA accelerator is used for accelerating SWExtend
    val fpgaSWExtThreshold = bwamemArgs.fpgaSWExtThreshold     // the threshold of using FPGA accelerator for SWExtend
    val jniSWExtendLibPath = bwamemArgs.jniSWExtendLibPath     // (optional) the JNI library path used for SWExtend FPGA acceleration

    // Initialize output writer
    val samWriter = new SAMWriter
    val samHDFSWriter = new SAMHDFSWriter(outputPath)
    if(outputChoice == SAM_OUT_LOCAL) {
      samWriter.init(outputPath)
      samWriter.writeString(samHeader.bwaGenSAMHeader(bwaIdx.bns, packageVersion))
    }
    else if(outputChoice == SAM_OUT_DFS) {
      samHDFSWriter.init
      samHDFSWriter.writeString(samHeader.bwaGenSAMHeader(bwaIdx.bns, packageVersion))
    }

    // broadcast shared variables
    // If each node has its own copy of human reference genome, we can bypass the broadcast from the driver node.
    // Otherwise, we need to use Spark broadcast
    var isLocalRef = false
    if(bwamemArgs.localRef == 1) 
      isLocalRef = true
    val bwaIdxGlobal = sc.broadcast(new ReferenceBroadcast(sc.broadcast(bwaIdx), isLocalRef, fastaLocalInputPath))

    val bwaMemOptGlobal = sc.broadcast(bwaMemOpt)

    // Used to avoid time consuming adamRDD.count (numProcessed += adamRDD.count)
    // Assume the number of read in one batch is the same (This is determined when uploading FASTQ to HDFS)
    val fastqRDDLoaderTmp = new FASTQRDDLoader(sc, fastqHDFSInputPath, fastqInputFolderNum)
    val rddTmp = fastqRDDLoaderTmp.PairEndRDDLoadOneBatch(0, batchFolderNum)
    val batchedReadNum = rddTmp.count
    rddTmp.unpersist(true)

    // *****   PROFILING    *******
    var worker1Time: Long = 0
    var calMetricsTime: Long = 0
    var worker2Time: Long = 0
    var ioWaitingTime: Long = 0

    var numProcessed: Long = 0
    // Process the reads in a batched fashion
    var i: Int = 0
    var folderID: Int = 0
    var isSAMWriteDone: Boolean = true  // a done signal for writing SAM file
    //var isFinalIteration: Boolean = false
    while(i < fastqInputFolderNum) {
      
      var pes: Array[MemPeStat] = new Array[MemPeStat](4)
      var j = 0
      while(j < 4) {
        pes(j) = new MemPeStat
        j += 1
      }

      // loading reads
      println("Load FASTQ files")
      val pairEndFASTQRDDLoader = new FASTQRDDLoader(sc, fastqHDFSInputPath, fastqInputFolderNum)
      val restFolderNum = fastqInputFolderNum - i
      var pairEndFASTQRDD: RDD[PairEndFASTQRecord] = null
      if(restFolderNum >= batchFolderNum) {
        pairEndFASTQRDD = pairEndFASTQRDDLoader.PairEndRDDLoadOneBatch(i, batchFolderNum)
        i += batchFolderNum
      }
      else {
        pairEndFASTQRDD = pairEndFASTQRDDLoader.PairEndRDDLoadOneBatch(i, restFolderNum)
        i += restFolderNum
        //isFinalIteration = true
      }

      // Worker1 (Map step)
      // *****   PROFILING    *******
      val startTime = System.currentTimeMillis

      println("@Worker1") 
      var reads: RDD[PairEndReadType] = null

      // SWExtend() is not processed in a batched way (by default)
      if(!isSWExtBatched) {
        reads = pairEndFASTQRDD.map( pairSeq => pairEndBwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bwt, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, null, pairSeq) ) 
      }
      // SWExtend() is processed in a batched way. FPGA accelerating may be applied
      else {
        def it2ArrayIt_W1(iter: Iterator[PairEndFASTQRecord]): Iterator[Array[PairEndReadType]] = {
          val batchedDegree = swExtBatchSize
          var counter = 0
          var ret: Vector[Array[PairEndReadType]] = scala.collection.immutable.Vector.empty
          var end1 = new Array[FASTQRecord](batchedDegree)
          var end2 = new Array[FASTQRecord](batchedDegree)
          
          while(iter.hasNext) {
            val pairEnd = iter.next
            end1(counter) = pairEnd.seq0
            end2(counter) = pairEnd.seq1
            counter += 1
            if(counter == batchedDegree) {
              ret = ret :+ pairEndBwaMemWorker1Batched(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bwt, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 
                                                 null, end1, end2, batchedDegree, isFPGAAccSWExtend, fpgaSWExtThreshold, jniSWExtendLibPath)
              counter = 0
            }
          }

          if(counter != 0) {
            ret = ret :+ pairEndBwaMemWorker1Batched(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bwt, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 
                                               null, end1, end2, counter, isFPGAAccSWExtend, fpgaSWExtThreshold, jniSWExtendLibPath)
          }

          ret.toArray.iterator
        }

        reads = pairEndFASTQRDD.mapPartitions(it2ArrayIt_W1).flatMap(s => s)
      }      

      pairEndFASTQRDD.unpersist(true)
      reads.cache

      // MemPeStat (Reduce step)
      val peStatPrepRDD = reads.map( pairSeq => memPeStatPrep(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns.l_pac, pairSeq) )
      val peStatPrepArray = peStatPrepRDD.collect

      // *****   PROFILING    *******
      val worker1EndTime = System.currentTimeMillis
      worker1Time += (worker1EndTime - startTime)

      memPeStatCompute(bwaMemOptGlobal.value, peStatPrepArray, pes)

      // *****   PROFILING    *******
      val calMetricsEndTime = System.currentTimeMillis
      calMetricsTime += (calMetricsEndTime - worker1EndTime)

      println("@MemPeStat")
      j = 0
      while(j < 4) {
        println("pes(" + j + "): " + pes(j).low + " " + pes(j).high + " " + pes(j).failed + " " + pes(j).avg + " " + pes(j).std)
        j += 1
      }
        
      // Check if the I/O thread has completed writing the output SAM file
      // If not, wait here!!!
      // This implementation use only worker1 stage to hide the I/O latency
      // It is slower but consume less memory footprint
      /*if(outputChoice == SAM_OUT_LOCAL) {
        println("[DEBUG] Main thread, Before while loop: isSAMWriteDone = " + isSAMWriteDone)

        while(!isSAMWriteDone) {
          try {
            println("Waiting for I/O")
            ioWaitingTime += 1
            Thread.sleep(1000)                 //1000 milliseconds is one second.
          } catch {
            case e: InterruptedException => Thread.currentThread().interrupt()
          }
        }

        println("[DEBUG] Main thread, After while loop: isSAMWriteDone = " + isSAMWriteDone)
        this.synchronized {
          isSAMWriteDone = false
        }
        println("[DEBUG] Main thread, Final value: isSAMWriteDone = " + isSAMWriteDone)

      }*/

      // Worker2 (Map step)
      println("@Worker2: Started")
      // NOTE: we may need to find how to utilize the numProcessed variable!!!
      // Batched Processing for P-SW kernel
      if(isPSWBatched) {
        // Not output SAM format file
        if(outputChoice == NO_OUT_FILE) {
          def it2ArrayIt(iter: Iterator[PairEndReadType]): Iterator[Unit] = {
            var counter = 0
            var ret: Vector[Unit] = scala.collection.immutable.Vector.empty
            var subBatch = new Array[PairEndReadType](subBatchSize)
            while (iter.hasNext) {
              subBatch(counter) = iter.next
              counter = counter + 1
              if (counter == subBatchSize) {
                ret = ret :+ pairEndBwaMemWorker2PSWBatched(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 0, pes, subBatch, subBatchSize, isPSWJNI, jniLibPath, samHeader) 
                counter = 0
              }
            }
            if (counter != 0)
              ret = ret :+ pairEndBwaMemWorker2PSWBatched(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 0, pes, subBatch, counter, isPSWJNI, jniLibPath, samHeader)
            ret.toArray.iterator
          }

          val count = reads.mapPartitions(it2ArrayIt).count
          println("Count: " + count)
 
          reads.unpersist(true)   // free RDD; seems to be needed (free storage information is wrong)
        }
        // Output SAM format file
        else if(outputChoice == SAM_OUT_LOCAL || outputChoice == SAM_OUT_DFS) {
          def it2ArrayIt(iter: Iterator[PairEndReadType]): Iterator[Array[Array[String]]] = {
            var counter = 0
            var ret: Vector[Array[Array[String]]] = scala.collection.immutable.Vector.empty
            var subBatch = new Array[PairEndReadType](subBatchSize)
            while (iter.hasNext) {
              subBatch(counter) = iter.next
              counter = counter + 1
              if (counter == subBatchSize) {
                ret = ret :+ pairEndBwaMemWorker2PSWBatchedSAMRet(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 0, pes, subBatch, subBatchSize, isPSWJNI, jniLibPath, samHeader) 
                counter = 0
              }
            }
            if (counter != 0)
              ret = ret :+ pairEndBwaMemWorker2PSWBatchedSAMRet(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 0, pes, subBatch, counter, isPSWJNI, jniLibPath, samHeader)
            ret.toArray.iterator
          }
 
          if(outputChoice == SAM_OUT_LOCAL) {
            println("@worker2")
            val samStrings = reads.mapPartitions(it2ArrayIt).collect
            println("worker2 done!")

            // This implementation can hide the I/O latency with both worker1 and worker2 stages
            // However, it requires 2X memory footprint on the driver node 
            // The program will crash if the memory (Java heap) on the driver node is not large enough
            // test
            println("[DEBUG] Main thread, Before while loop: isSAMWriteDone = " + isSAMWriteDone)

            while(!isSAMWriteDone) {
              try {
                println("Waiting for I/O")
                ioWaitingTime += 1
                Thread.sleep(1000)                 //1000 milliseconds is one second.
              } catch {
                case e: InterruptedException => Thread.currentThread().interrupt()
              }
            }

            println("[DEBUG] Main thread, After while loop: isSAMWriteDone = " + isSAMWriteDone)
            this.synchronized {
              isSAMWriteDone = false
            }
            println("[DEBUG] Main thread, Final value: isSAMWriteDone = " + isSAMWriteDone)
            // end of test

            println("Count: " + samStrings.size)
            reads.unpersist(true)   // free RDD; seems to be needed (free storage information is wrong)
 
            val f: Future[Int] = Future {
              samStrings.foreach(s => {
                s.foreach(pairSeq => {
                  samWriter.writeString(pairSeq(0))
                  samWriter.writeString(pairSeq(1))
                } )
              } )
              //samWriter.flush
              1
            }

            f onComplete {
              case Success(s) => {
                println("[DEBUG] Forked thread, Before: isSAMWriteDone = " + isSAMWriteDone)
                println("Successfully write the SAM strings to a local file: " + s)
                this.synchronized {
                  isSAMWriteDone = true
                }
                println("[DEBUG] Forked thread, After: isSAMWriteDone = " + isSAMWriteDone)
                
                /*if(isFinalIteration) {
                  println("[DEBUG] Final iteration, Close samWriter")
                  samWriter.close
                  val today = Calendar.getInstance().getTime()
                  // create the date/time formatters
                  val minuteFormat = new SimpleDateFormat("mm")
                  val hourFormat = new SimpleDateFormat("hh")
                  val secondFormat = new SimpleDateFormat("ss")
                  val currentHour = hourFormat.format(today)      // 12
                  val currentMinute = minuteFormat.format(today)  // 29
                  val currentSecond = secondFormat.format(today)  // 50
                  println("samWriter is closed: " + currentHour + ":" + currentMinute + ":" + currentSecond)
                }*/
              }
              case Failure(f) => println("An error has occured: " + f.getMessage)
            }

            /*if(isFinalIteration) {
              println("Main thread: waiting for closing samWriter (wait for at most 1000 seconds)")
              Await.result(f, 1000.second)
              println("Main thread: samWriter closed!")
            }*/
          }
          else if(outputChoice == SAM_OUT_DFS) {
            val samStrings = reads.mapPartitions(it2ArrayIt).flatMap(s => s).map(pairSeq => pairSeq(0) + pairSeq(1))
            reads.unpersist(true)
            samStrings.saveAsTextFile(outputPath + "/body")
          }
        }
        // Output ADAM format file
        else if(outputChoice == ADAM_OUT) {
          def it2ArrayIt(iter: Iterator[PairEndReadType]): Iterator[Array[AlignmentRecord]] = {
            var counter = 0
            var ret: Vector[Array[AlignmentRecord]] = scala.collection.immutable.Vector.empty
            var subBatch = new Array[PairEndReadType](subBatchSize)
            while (iter.hasNext) {
              subBatch(counter) = iter.next
              counter = counter + 1
              if (counter == subBatchSize) {
                ret = ret :+ pairEndBwaMemWorker2PSWBatchedADAMRet(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 0, pes, subBatch, subBatchSize, isPSWJNI, jniLibPath, samHeader, seqDict, readGroup) 
                counter = 0
              }
            }
            if (counter != 0)
              ret = ret :+ pairEndBwaMemWorker2PSWBatchedADAMRet(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 0, pes, subBatch, counter, isPSWJNI, jniLibPath, samHeader, seqDict, readGroup)
            ret.toArray.iterator
          }
 
          //val adamObjRDD = sc.union(reads.mapPartitions(it2ArrayIt))
          val adamObjRDD = reads.mapPartitions(it2ArrayIt).flatMap(r => r)
          adamObjRDD.adamParquetSave(outputPath + "/"  + folderID.toString())
          println("@Worker2: Completed")
          numProcessed += batchedReadNum
          folderID += 1
          reads.unpersist(true)
          adamObjRDD.unpersist(true)  // free RDD; seems to be needed (free storage information is wrong)         
        }
      }
      // NOTE: need to be modified!!!
      // Normal read-based processing
      else {
        val count = reads.map(pairSeq => pairEndBwaMemWorker2(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 0, pes, pairSeq, samHeader) ).count
        numProcessed += count.toLong
      }
 
      // *****   PROFILING    *******
      val worker2EndTime = System.currentTimeMillis
      worker2Time += (worker2EndTime - calMetricsEndTime)
    }

   
    if(outputChoice == SAM_OUT_LOCAL) {
      println("[DEBUG] Main thread, Final iteration, Before: isSAMWriteDone = " + isSAMWriteDone)
      while(!isSAMWriteDone) {
        try {
          println("Waiting for I/O, at final iteration")
          ioWaitingTime += 1
          Thread.sleep(1000)                 //1000 milliseconds is one second.
        } catch {
          case e: InterruptedException => Thread.currentThread().interrupt()
        }
      }
      println("[DEBUG] Main thread, Final iteration, After: isSAMWriteDone = " + isSAMWriteDone)
      //samWriter.flush
      samWriter.close
      
      val today = Calendar.getInstance().getTime()
      // create the date/time formatters
      val minuteFormat = new SimpleDateFormat("mm")
      val hourFormat = new SimpleDateFormat("hh")
      val secondFormat = new SimpleDateFormat("ss")
      val currentHour = hourFormat.format(today)      // 12
      val currentMinute = minuteFormat.format(today)  // 29
      val currentSecond = secondFormat.format(today)  // 50
      println("SAMWriter close: " + currentHour + ":" + currentMinute + ":" + currentSecond)
    }
    else if(outputChoice == SAM_OUT_DFS)
      samHDFSWriter.close
    
    println("Summary:")
    println("Worker1 Time: " + worker1Time)
    println("Calculate Metrics Time: " + calMetricsTime)
    println("Worker2 Time: " + worker2Time)
    println("I/O waiting time for writing data to the disk (for local SAM format only): " + ioWaitingTime)
    sc.stop
  }


  /**
    *  memSingleEndMapping: the main function to perform single-end read mapping
    *
    *  @param sc the spark context object
    *  @param fastaLocalInputPath the local BWA index files (bns, pac, and so on)
    *  @param fastqHDFSInputPath the raw read file stored in HDFS
    *  @param fastqInputFolderNum the number of folders generated in the HDFS for the raw reads
    *  @param batchFolderNum the number of raw read folders in a batch to be processed
    *  @param bwaMemOpt the MemOptType object
    *  @param bwaIdx the BWAIdxType object
    *  @param outputChoice the output format choice
    *  @param outputPath the output path in the local or distributed file system
    *  @param samHeader the SAM header file used for writing SAM output file
    *  @param seqDict (optional) the sequences (chromosome) dictionary: used for ADAM format output
    *  @param readGroup (optional) the read group: used for ADAM format output
    */
  private def memSingleEndMapping(sc: SparkContext, fastaLocalInputPath: String, fastqHDFSInputPath: String, fastqInputFolderNum: Int, batchFolderNum: Int, 
                                  bwaMemOpt: MemOptType, bwaIdx: BWAIdxType, outputChoice: Int, outputPath: String, samHeader: SAMHeader,
                                  seqDict: SequenceDictionary = null, readGroup: RecordGroup = null) 
  {
    // Initialize output writer
    val samWriter = new SAMWriter
    val samHDFSWriter = new SAMHDFSWriter(outputPath)
    if(outputChoice == SAM_OUT_LOCAL) {
      samWriter.init(outputPath)
      samWriter.writeString(samHeader.bwaGenSAMHeader(bwaIdx.bns, packageVersion))
    }
    else if(outputChoice == SAM_OUT_DFS) {
      samHDFSWriter.init
      samHDFSWriter.writeString(samHeader.bwaGenSAMHeader(bwaIdx.bns, packageVersion))
    }

    // broadcast shared variables
    //val bwaIdxGlobal = sc.broadcast(bwaIdx, fastaLocalInputPath)  // read from local disks!!!
    val bwaIdxGlobal = sc.broadcast(bwaIdx)  // broadcast
    val bwaMemOptGlobal = sc.broadcast(bwaMemOpt)
    val fastqRDDLoader = new FASTQRDDLoader(sc, fastqHDFSInputPath, fastqInputFolderNum)

    // Not output SAM file
    // For runtime estimation
    if(outputChoice == NO_OUT_FILE) {
      // loading reads
      println("Load FASTQ files")
      val fastqRDD = fastqRDDLoader.RDDLoadAll

      println("@Worker1")
      val reads = fastqRDD.map( seq => bwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, seq) )
      println("@Worker2")
      val c = reads.map( r => singleEndBwaMemWorker2(bwaMemOptGlobal.value, r.regs, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, r.seq, 0, samHeader) ).count
      println("Count: " + c)
    }
    // output SAM file
    else if(outputChoice == SAM_OUT_LOCAL || outputChoice == SAM_OUT_DFS) {
      var numProcessed: Long = 0

      // Process the reads in a batched fashion
      var i: Int = 0
      while(i < fastqInputFolderNum) {
        val restFolderNum = fastqInputFolderNum - i
        var singleEndFASTQRDD: RDD[FASTQRecord] = null
        if(restFolderNum >= batchFolderNum) {
          singleEndFASTQRDD = fastqRDDLoader.SingleEndRDDLoadOneBatch(i, batchFolderNum)
          i += batchFolderNum
        }
        else {
          singleEndFASTQRDD = fastqRDDLoader.SingleEndRDDLoadOneBatch(i, restFolderNum)
          i += restFolderNum
        }

        // Write to an output file in the local file system in a sequencial way 
        if(outputChoice == SAM_OUT_LOCAL) {
          // worker1, worker2, and return SAM format strings
          val samStrings = singleEndFASTQRDD.map(seq => bwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, seq) )
                                            .map(r => singleEndBwaMemWorker2(bwaMemOptGlobal.value, r.regs, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, r.seq, numProcessed, samHeader) )
                                            .collect
          numProcessed += samStrings.size
          samWriter.writeStringArray(samStrings)
          //samWriter.flush
        }
        // Write to HDFS
        else if(outputChoice == SAM_OUT_DFS) {
          // worker1, worker2, and return SAM format strings
          val samStrings = singleEndFASTQRDD.map(seq => bwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, seq) )
                                            .map(r => singleEndBwaMemWorker2(bwaMemOptGlobal.value, r.regs, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, r.seq, numProcessed, samHeader) )
          samStrings.saveAsTextFile(outputPath + "/body")
        }
 
        singleEndFASTQRDD.unpersist(true)
      }

      if(outputChoice == SAM_OUT_LOCAL)
        samWriter.close
      else if(outputChoice == SAM_OUT_DFS)
        samHDFSWriter.close
    } 
    // output ADAM format to the distributed file system
    else if(outputChoice == ADAM_OUT) {
      var numProcessed: Long = 0

      // Used to avoid time consuming adamRDD.count (numProcessed += adamRDD.count)
      // Assume the number of read in one batch is the same (This is determined when uploading FASTQ to HDFS)
      val fastqRDDLoaderTmp = new FASTQRDDLoader(sc, fastqHDFSInputPath, fastqInputFolderNum)
      val rddTmp = fastqRDDLoaderTmp.SingleEndRDDLoadOneBatch(0, batchFolderNum)
      val batchedReadNum = rddTmp.count
      rddTmp.unpersist(true)

      // Process the reads in a batched fashion
      var i: Int = 0
      var folderID: Int = 0
      while(i < fastqInputFolderNum) {
        val restFolderNum = fastqInputFolderNum - i
        var singleEndFASTQRDD: RDD[FASTQRecord] = null
        if(restFolderNum >= batchFolderNum) {
          singleEndFASTQRDD = fastqRDDLoader.SingleEndRDDLoadOneBatch(i, batchFolderNum)
          i += batchFolderNum
        }
        else {
          singleEndFASTQRDD = fastqRDDLoader.SingleEndRDDLoadOneBatch(i, restFolderNum)
          i += restFolderNum
        }

        // worker1, worker2, and return SAM format strings
        val adamRDD = singleEndFASTQRDD.map(seq => bwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.bwt, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, null, seq) )
                                       .flatMap(r => singleEndBwaMemWorker2ADAMOut(bwaMemOptGlobal.value, r.regs, bwaIdxGlobal.value.bns, bwaIdxGlobal.value.pac, 
                                                                                   r.seq, numProcessed, samHeader, seqDict, readGroup) )
                                          
        adamRDD.adamParquetSave(outputPath + "/"  + folderID.toString())
        numProcessed += batchedReadNum
        folderID += 1

        singleEndFASTQRDD.unpersist(true)
        adamRDD.unpersist(true)
      }
    }
    else {
      println("[Error] Undefined output choice" + outputChoice)
      exit(1)
    }

  }

} 
