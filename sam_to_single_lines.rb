#!/usr/bin/ruby1.9
require 'wukong'

# Given a SAM file, make up something new where the pairs are together one one
# line. Also we only care about the name, sequence, quality, bit flags, and maps
module SamToSingleLines
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
  
  def self.test_bit(bit,mask)
    mask == (bit & mask)
  end

  class Mapper < Wukong::Streamer::LineStreamer

    # input is a line from a SAM file, we are only concerned with non headaers
    def process line
      return if line =~ /^@/
      parts = line.chomp.split(/\t/)
      yield [parts[0], *parts]
    end

  end

  class Reducer < Wukong::Streamer::ListReducer
    NAME_IDX = 0
    BIT_IDX = 1
    CHR_IDX = 2
    POS_IDX = 3
    SEQ_IDX = 9
    QUALITY_IDX = 10
    
    # values is an array of key (the name), SAM fields (minues name)
    # we'll get now a line with the name, bit, chr, pos, seq, qual, bit, chr, pos, seq, qual
    def finalize
      return unless 2 == values.size
      values.sort! {|a,b| a[BIT_IDX].to_i <=> b[BIT_IDX].to_i}
      res [ key ]
      values.each do |v|
        res += [ v[BIT_IDX], v[CHR_IDX], v[POS_IDX], v[SEQ_IDX], v[QUALITY_IDX] ]
      end
      yield [ res ]
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  SamToSingleLines::Mapper,
  SamToSingleLines::Reducer
  ).run # Execute the script