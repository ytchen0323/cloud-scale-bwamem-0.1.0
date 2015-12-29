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


package cs.ucla.edu.bwaspark.worker2

import scala.List
import scala.math.abs

import java.util.ArrayList
import java.util.Collections
import java.util.Comparator

import cs.ucla.edu.bwaspark.datatype._

//MemMarkPrimarySe
object MemMarkPrimarySe {
  
  
  /**
   * The main function of memMarkPrimarySe class
   *
   * @param opt the MemOptType object
   * @param a the MemAlnRegType object
   * @param id the Long object
   */
  def memMarkPrimarySe(opt: MemOptType, a: ArrayList[MemAlnRegType], id: Long) :
      ArrayList[MemAlnRegType] = {
    var n: Int = 0
    if(a != null) n = a.size
    var i: Int = 0
    var j: Int = 0
    var tmp: Int = 0
    var k: Int = 0
    var z: Array[Int] = new Array[Int](n)
    var zIdx = 0
    //aVar, the returned value
    var aVar: ArrayList[MemAlnRegType] = null
    if(n != 0) {
      i = 0
      while(i < n) {
        a.get(i).sub = 0
        a.get(i).secondary = -1
        a.get(i).hash = hash64((id + i.toLong))
        i += 1
      }
      //ks_introsort(mem_ars_hash, n, a)
      //#define alnreg_hlt(a, b) ((a).score > (b).score || ((a).score == (b).score && (a).hash < (b).hash))
      //aVar = a.sortWith( (x, y) => ((x.score > y.score) || ( x.score == y.score && (x.hash >>> 1) < (y.hash >>> 1) )  ) )
      Collections.sort(a, new Comparator[MemAlnRegType] {
        override def compare(o1 : MemAlnRegType, o2 : MemAlnRegType) : Int = {
          if (o1.score > o2.score) -1
          else if (o1.score < o2.score) 1
          else {
            if (o1.hash < o2.hash) -1
            else if (o1.hash > o2.hash) 1
            else 0
          }
        }
        override def equals(obj : Any) : Boolean = { false }
      })
      aVar = a
      tmp = opt.a + opt.b
      if((opt.oDel + opt.eDel) > tmp) {
        tmp = opt.oDel + opt.eDel
      }
      if((opt.oIns + opt.eIns) > tmp) {
        tmp = opt.oIns + opt.eIns
      }
      //kv_push()
      z(0) = 0
      zIdx += 1
      i = 1
      while(i < n) {
        var breakIdx: Int = zIdx
        var isBreak = false
        k = 0
       
        while(k < zIdx && !isBreak) {
          j = z(k)
          var bMax: Int = if(aVar.get(j).qBeg > aVar.get(i).qBeg) aVar.get(j).qBeg else aVar.get(i).qBeg
          var eMin: Int = if(aVar.get(j).qEnd < aVar.get(i).qEnd) aVar.get(j).qEnd else aVar.get(i).qEnd
          // have overlap
          if( eMin > bMax ) {
            var minL: Int = if ((aVar.get(i).qEnd - aVar.get(i).qBeg)<(aVar.get(j).qEnd - aVar.get(j).qBeg)) (aVar.get(i).qEnd - aVar.get(i).qBeg) else (aVar.get(j).qEnd - aVar.get(j).qBeg)
            //have significant overlap
            if((eMin - bMax)>= minL * opt.maskLevel) {
              if(aVar.get(j).sub == 0) {
                aVar.get(j).sub = aVar.get(i).score
              }
              if((aVar.get(j).score - aVar.get(i).score) <= tmp) aVar.get(j).subNum = aVar.get(j).subNum + 1
              breakIdx = k
              isBreak = true
            }
          }

          k += 1
        }

        if(breakIdx == zIdx) {
          z(zIdx) = i
          zIdx += 1
        }
        else {
          aVar.get(i).secondary = z(k)
        }

        i += 1
      }
    }
    aVar
  }

  def hash64( key: Long ) : Long = {
    var keyVar: Long = key
    keyVar += ~(keyVar << 32)
    keyVar ^= (keyVar >>> 22)
    keyVar += ~(keyVar << 13)
    keyVar ^= (keyVar >>> 8)
    keyVar += (keyVar << 3)
    keyVar ^= (keyVar >>> 15)
    keyVar += ~(keyVar <<27)
    keyVar ^= (keyVar >>> 31)
    keyVar
  }
}
