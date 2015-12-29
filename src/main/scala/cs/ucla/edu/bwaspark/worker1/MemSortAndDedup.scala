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


package cs.ucla.edu.bwaspark.worker1

import scala.util.control.Breaks._

import java.util.ArrayList
import java.util.Collections
import java.util.Comparator

import cs.ucla.edu.bwaspark.datatype._

object MemSortAndDedup {
  /**
    *  Sort the MemAlnRegs according to the given order
    *  and remove the redundant MemAlnRegs
    *  
    *  @param regArray alignment registers, which are the output of chain to alignment (after memChainToAln() is applied)
    *  @param maskLevelRedun mask level of redundant alignment registers (from MemOptType object)
    */
  def memSortAndDedup(regs: ArrayList[MemAlnRegType], maskLevelRedun: Float):
        ArrayList[MemAlnRegType] = {
    var count : Int = 0
    while (count < regs.size && regs.get(count) != null) {
      count += 1
    }

    if (count <= 1) {
      regs
    } else {
      val comparator = new Comparator[MemAlnRegType] {
        override def compare(o1 : MemAlnRegType, o2 : MemAlnRegType) : Int = {
          if (o1.score > o2.score) {
            -1
          } else if (o1.score < o2.score) {
            1
          } else {
            if (o1.rBeg < o2.rBeg) {
              -1
            } else if (o1.rBeg > o2.rBeg) {
              1
            } else {
              if (o1.qBeg < o2.qBeg) {
                -1
              } else if (o1.qBeg > o2.qBeg) {
                1
              } else {
                0
              }
            }
          }
        }

        override def equals(obj : Any) : Boolean = {
          false
        }
      }

      Collections.sort(regs, comparator)

      var i = 1
      while (i < regs.size) {
        if(regs.get(i).rBeg < regs.get(i-1).rEnd) {
          var j = i - 1
          var isBreak = false
          while(j >= 0 && regs.get(i).rBeg < regs.get(j).rEnd && !isBreak) {
            // a[j] has been excluded
            if(regs.get(j).qEnd != regs.get(j).qBeg) { 
              var oq = 0
              var mr: Long = 0
              var mq = 0
              var or = regs.get(j).rEnd - regs.get(i).rBeg // overlap length on the reference
              // overlap length on the query
              if(regs.get(j).qBeg < regs.get(i).qBeg) oq = regs.get(j).qEnd - regs.get(i).qBeg
              else oq = regs.get(i).qEnd - regs.get(j).qBeg
              // min ref len in alignment
              if(regs.get(j).rEnd - regs.get(j).rBeg < regs.get(i).rEnd - regs.get(i).rBeg) mr = regs.get(j).rEnd - regs.get(j).rBeg
              else mr = regs.get(i).rEnd - regs.get(i).rBeg
              // min qry len in alignment
              if(regs.get(j).qEnd - regs.get(j).qBeg < regs.get(i).qEnd - regs.get(i).qBeg) mq = regs.get(j).qEnd - regs.get(j).qBeg
              else mq = regs.get(i).qEnd - regs.get(i).qBeg
              // one of the hits is redundant
              if(or > maskLevelRedun * mr && oq > maskLevelRedun * mq) {
                if(regs.get(i).score < regs.get(j).score) {
                  regs.get(i).qEnd = regs.get(i).qBeg
                  isBreak = true
                } else {
                  regs.get(j).qEnd = regs.get(j).qBeg
                }
              }
            }             
 
            j -= 1
          }

        }

        i += 1
      }

      // exclude identical hits
      val filtered1 = new ArrayList[MemAlnRegType](regs.size)
      for (i <- 0 until regs.size) {
        val r = regs.get(i)
        if (r.qEnd > r.qBeg) filtered1.add(r)
      }

      Collections.sort(filtered1, comparator)

      i = 1
      while(i < filtered1.size) {
        if(filtered1.get(i).score == filtered1.get(i-1).score && filtered1.get(i).rBeg == filtered1.get(i-1).rBeg &&
            filtered1.get(i).qBeg == filtered1.get(i-1).qBeg)
          filtered1.get(i).qEnd = filtered1.get(i).qBeg
        i += 1
      }        

      val final_regs = new ArrayList[MemAlnRegType](filtered1.size)
      for (i <- 0 until filtered1.size) {
        val r = filtered1.get(i)
        if (r.qEnd > r.qBeg) final_regs.add(r)
      }

      final_regs
    }
  }
}

