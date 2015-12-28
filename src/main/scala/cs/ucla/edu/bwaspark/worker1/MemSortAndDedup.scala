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

import cs.ucla.edu.bwaspark.datatype._

object MemSortAndDedup {
  /**
    *  Sort the MemAlnRegs according to the given order
    *  and remove the redundant MemAlnRegs
    *  
    *  @param regArray alignment registers, which are the output of chain to alignment (after memChainToAln() is applied)
    *  @param maskLevelRedun mask level of redundant alignment registers (from MemOptType object)
    */
  def memSortAndDedup(inRegs: Array[MemAlnRegType], maskLevelRedun: Float):
        Array[MemAlnRegType] = {
    var count : Int = 0
    while (count < inRegs.length && inRegs(count) != null) {
      count += 1
    }

    if (count <= 1) {
      inRegs
    } else {
      var regs = inRegs.sortBy(r => (r.rEnd, r.rBeg))

      var i = 1
      while (i < regs.length) {
        if(regs(i).rBeg < regs(i-1).rEnd) {
          var j = i - 1
          var isBreak = false
          while(j >= 0 && regs(i).rBeg < regs(j).rEnd && !isBreak) {
            // a[j] has been excluded
            if(regs(j).qEnd != regs(j).qBeg) { 
              var oq = 0
              var mr: Long = 0
              var mq = 0
              var or = regs(j).rEnd - regs(i).rBeg // overlap length on the reference
              // overlap length on the query
              if(regs(j).qBeg < regs(i).qBeg) oq = regs(j).qEnd - regs(i).qBeg
              else oq = regs(i).qEnd - regs(j).qBeg
              // min ref len in alignment
              if(regs(j).rEnd - regs(j).rBeg < regs(i).rEnd - regs(i).rBeg) mr = regs(j).rEnd - regs(j).rBeg
              else mr = regs(i).rEnd - regs(i).rBeg
              // min qry len in alignment
              if(regs(j).qEnd - regs(j).qBeg < regs(i).qEnd - regs(i).qBeg) mq = regs(j).qEnd - regs(j).qBeg
              else mq = regs(i).qEnd - regs(i).qBeg
              // one of the hits is redundant
              if(or > maskLevelRedun * mr && oq > maskLevelRedun * mq) {
                if(regs(i).score < regs(j).score) {
                  regs(i).qEnd = regs(i).qBeg
                  isBreak = true
                } else {
                  regs(j).qEnd = regs(j).qBeg
                }
              }
            }             
 
            j -= 1
          }

        }

        i += 1
      }

      // exclude identical hits
      regs = regs.filter(r => (r.qEnd > r.qBeg))

      regs = regs.sortBy(r => (- r.score, r.rBeg, r.qBeg))
      
      i = 1
      while(i < regs.length) {
        if(regs(i).score == regs(i-1).score && regs(i).rBeg == regs(i-1).rBeg &&
            regs(i).qBeg == regs(i-1).qBeg)
          regs(i).qEnd = regs(i).qBeg
        i += 1
      }        

      regs = regs.filter(r => (r.qEnd > r.qBeg))

      regs
    }
  }
}

