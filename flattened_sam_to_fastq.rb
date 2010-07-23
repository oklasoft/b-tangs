#!/usr/bin/ruby1.9
require 'wukong'

class String
  def complement
    self.each_char.inject('') do |accum,c| 
      accum += case c
        when 'A'
          'T'
        when 'C'
          'G'
        when 'G'
          'C'
        when 'T'
          'A'
        else
          c
      end
    end
  end
end

module FlattenedSAMToFastq
  IS_READ_PAIRED_ALIGNED = 2
  IS_UNMAPPED = 4
  IS_REVERSED_STRAND = 16
  IS_FIRST_READ = 64
  IS_SECOND_READ = 128
  
  # sort them based on the bit mask
  # the one that the reverse strand will be sorted lower
  # if they are both reverse or both forward, the second read in the pair is
  # lower
  # [83,163].sort {|a,b| SamToSingleLines.reverse_bit_after_forward(a,b)} >> 163,83
  # [147,99].sort {|a,b| SamToSingleLines.reverse_bit_after_forward(a,b)} >> 99,147
  # [163,99].sort {|a,b| SamToSingleLines.reverse_bit_after_forward(a,b)} >> 99,163
  def self.reverse_bit_after_forward(a,b)
    a_is_reversed = is_reversed?(a)
    b_is_reversed = is_reversed?(b)
    
    return 1 if a_is_reversed && ! b_is_reversed
    return -1 if b_is_reversed && ! a_is_reversed
    
    return -1 if is_first_read(a) && is_second_read(b)
    return 1 if is_first_read(b) && is_second_read(a)
    
    return 0 if a == b
  end
  
  def self.is_reversed?(bit)
    test_bit(bit,IS_REVERSED_STRAND)
  end
  
  def self.is_first_read(bit)
    test_bit(bit,IS_FIRST_READ)
  end
  
  def self.is_second_read(bit)
    test_bit(bit,IS_SECOND_READ)
  end

  def self.is_aligned_pair(bit)
    test_bit(bit,IS_READ_PAIRED_ALIGNED)
  end

  def self.is_mapped(bit)
    !test_bit(bit,IS_UNMAPPED)
  end
  
  def self.test_bit(bit,mask)
    mask == (bit & mask)
  end

  NAME_IDX = 0
  BIT_ONE_IDX = 1
  CHR_ONE_IDX = 2
  POS_ONE_IDX = 3
  SEQ_ONE_IDX = 4
  QUALITY_ONE_IDX = 5
  FIRST = [BIT_ONE_IDX,SEQ_ONE_IDX,QUALITY_ONE_IDX, CHR_ONE_IDX, POS_ONE_IDX]

  BIT_TWO_IDX = 6
  CHR_TWO_IDX = 7
  POS_TWO_IDX = 8
  SEQ_TWO_IDX = 9
  QUALITY_TWO_IDX = 10
  SECOND = [BIT_TWO_IDX,SEQ_TWO_IDX,QUALITY_TWO_IDX, CHR_TWO_IDX,POS_TWO_IDX]
  
  class Mapper < Wukong::Streamer::LineStreamer
    
    SAM_TO_ILLUMINA_QUALITY_OFFSET = 31
    
    # input is a 'flattened SAM' file
    # name bit chr pos seq qual bit chr pos seq qual
    # then you can:
    # awk -F '\t' '{print "@"$1"/1\n"$2"\n+"$1"/1\n"$3}'
    # awk -F '\t' '{print "@"$1"/2\n"$4"\n+"$1"/2\n"$5}'
    def process line
      parts = line.chomp.split(/\t/)
      order = [FIRST, SECOND]

      if FlattenedSAMToFastq.is_second_read(parts[BIT_ONE_IDX].to_i)
        order.reverse!
      end
      res = [ parts[0] ]
      order.each do |fields|
        seq = parts[fields[1]]
        quality = illumina_quality_string(parts[fields[2]])
        
        if FlattenedSAMToFastq.is_reversed?(parts[fields[0]].to_i)
          seq = seq.complement.reverse
          quality.reverse!
        end
        res += [seq, quality]
        if options[:annotations]
          # add the right bit flag, chr & pos
          res += [parts[fields[0]], parts[fields[3]], parts[fields[4]]]
        end
      end
      yield res
    end
    
    def illumina_quality_string(quality)
      quality.each_char.inject('') {|sum,char| sum += (char.ord+SAM_TO_ILLUMINA_QUALITY_OFFSET).chr}
    end

  end


end
    
Wukong::Script.new(
  FlattenedSAMToFastq::Mapper,
  nil
  ).run # Execute the script