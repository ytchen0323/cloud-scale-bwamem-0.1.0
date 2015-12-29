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


package cs.ucla.edu.bwaspark.datatype

import cs.ucla.edu.avro.fastq._

import org.apache.avro.io._
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.ObjectStreamException

class ExtParam() {
	var leftQs: Array[Byte] = _
	var leftQlen: Int = -1
	var leftRs: Array[Byte] = _
	var leftRlen: Int = -1
	var rightQs: Array[Byte] = _
	var rightQlen: Int = -1
	var rightRs: Array[Byte] = _
	var rightRlen: Int = -1
	var w: Int = -1
	var mat: Array[Byte] = _
	var oDel: Int = -1
	var eDel: Int = -1
	var oIns: Int = -1
	var eIns: Int = -1
	var penClip5: Int = -1
	var penClip3: Int = -1
	var zdrop: Int = -1
	var h0: Int = -1
	var regScore: Int = -1
	var qBeg: Int = -1;
	//var rBeg: Long = -1l;
	//var qe: Int = -1;
	//var re: Long = -1l;
	var idx: Int = -1
	//var rmax0: Long = -1l
	def display() {
		println("leftQlen: " + leftQlen)
    if (leftQlen > 0) leftQs.foreach(ele => {print(ele + " ")})
		println()
		println("leftRlen: " + leftRlen)
    if (leftRlen > 0) leftRs.foreach(ele => {print(ele + " ")})
		println()
		println("rightQlen: " + rightQlen)
    if (rightQlen > 0 ) rightQs.foreach(ele => {print(ele + " ")})
		println()
    println("rightRlen: " + rightRlen)
		if (rightRlen > 0) rightRs.foreach(ele => {print(ele + " ")})
		println()
		println("w: " + w)
		println("oDel: " + oDel)
		println("eDel: " + eDel)
		println("oIns: " + oIns)
		println("eIns: " + eIns)
		println("penClip5: " + penClip5)
		println("penClip3: " + penClip3)
		println("zdrop: " + zdrop)
		println("h0: " + h0)
		println("regScore: " + regScore)
		println("qBeg: " + qBeg)
		//println("rBeg: " + rBeg)
		//println("qe: " + qe)
		//println("re: " + re)
		println("idx: " + idx)
		//println("rmax0: " + rmax0)
	}
}

@serializable
class ExtRet() {
	var qBeg: Int = -1
	var rBeg: Long = -1
	var qEnd: Int = -1
	var rEnd: Long = -1
	var score: Int = -1
	var trueScore: Int = -1
	var width: Int = -1
	var idx: Int = -1

    var partitionId : Long = -1
    var numOfReads : Int = -1
    var regFlag : Boolean = false
    var maxLength : Int = 0
    var seedArray_rBeg : Long = 0
    var seedArray_qBeg : Int = 0
    var seedArray_len : Int = 0

	def display() {
		println("qBeg: " + qBeg)
		println("rBeg: " + rBeg)
		println("qEnd: " + qEnd)
		println("rEnd: " + rEnd)
		println("score: " + score)
		println("trueScore: " + trueScore)
		println("width: " + width)
		println("idx: " + idx)
	}
}

class ExtMetadata(var chainFiltered : MemChainType, var end0 : Boolean,
    var seq : FASTQRecord) extends Serializable {
  def getChainFiltered() : MemChainType = { chainFiltered }
  def getEnd0() : Boolean = { end0 }
  def getSeq() : FASTQRecord = { seq }

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(chainFiltered)
    out.writeBoolean(end0)

    val writer = new SpecificDatumWriter[FASTQRecord](classOf[FASTQRecord])
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    writer.write(seq, encoder)
    encoder.flush()
  }

  private def readObject(in: ObjectInputStream) {
    chainFiltered = in.readObject.asInstanceOf[MemChainType]
    end0 = in.readBoolean

    val reader = new SpecificDatumReader[FASTQRecord](classOf[FASTQRecord])
    val decoder = DecoderFactory.get.binaryDecoder(in, null)
    seq = reader.read(null, decoder).asInstanceOf[FASTQRecord]
  }

  private def readObjectNoData() {
  }
}
