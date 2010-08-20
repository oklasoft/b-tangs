class String
  
  # sam : ASCII - 33
  # illuma fastq : ASCII - 64
  def phred_quality_score_sum
    self.each_char.inject(0.0) {|sum,char| sum += (char.ord-64)}
  end

  def phred_quality_score_average
    self.phred_quality_score_sum/self.length
  end
end