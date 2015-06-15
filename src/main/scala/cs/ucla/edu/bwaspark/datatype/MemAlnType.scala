package cs.ucla.edu.bwaspark.datatype

class MemAlnType {
  var pos: Long = 0          // forward strand 5'-end mapping position
  var rid: Int = 0           // reference sequence index in bntseq_t; <0 for unmapped
  var flag: Int = 0          // extra flag
  var isRev: Byte = 0        // is_rev: whether on the reverse strand
  var mapq: Short = 0        // mapq: mapping quality
  var NM: Int = 0            // NM: edit distance
  var nCigar: Int = 0        // number of CIGAR operations
  var cigar: CigarType = _   // CIGAR in the BAM encoding: opLen<<4|op; op to integer mapping: MIDSH=>01234
  var score: Int = 0
  var sub: Int = 0

  /**
    *  Make a copy of the current object
    */
  def copy(): MemAlnType = {
    var aln = new MemAlnType
    aln.pos = pos
    aln.rid = rid
    aln.flag = flag
    aln.isRev = isRev
    aln.mapq = mapq
    aln.NM = NM
    aln.nCigar = nCigar
    if(nCigar > 0)
      aln.cigar = cigar.copy
    aln.score = score
    aln.sub = sub
    aln
  }
}

