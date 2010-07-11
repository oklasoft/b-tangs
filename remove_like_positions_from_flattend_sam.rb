#!/usr/bin/ruby1.9
require 'wukong'

# Given a SAM file, make up something new where the pairs are together one one
# line. Also we only care about the name, sequence, quality, bit flags, and maps
module RemoveLikePositionsFromFlattenedSAM
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
  BIT_TWO_IDX = 6
  CHR_TWO_IDX = 7
  POS_TWO_IDX = 8
  SEQ_TWO_IDX = 9
  QUALITY_TWO_IDX = 10

  class Mapper < Wukong::Streamer::LineStreamer
    
    # input is a 'flattened SAM' file
    # name bit chr pos seq qual bit chr pos seq qual
    def process line
      parts = line.chomp.split(/\t/)
      key = nil
      unless RemoveLikePositionsFromFlattenedSAM.is_mapped(parts[BIT_ONE_IDX].to_i)
        key = "UNMAPPED_#{rand(options[:reduce_tasks].to_i).to_i}" # for splitting up this otherwise large key to be reduced
        return if options[:remove_unmapped]
      else
        key = parts[RemoveLikePositionsFromFlattenedSAM::CHR_ONE_IDX..RemoveLikePositionsFromFlattenedSAM::POS_ONE_IDX].join("_")
      end

      yield [key, *parts]
    end

  end

  class Reducer < Wukong::Streamer::ListReducer
    
    def average_quality_score(quality_string)
      quality_string.each_char.inject(0.0) {|sum,char| sum += (char.ord-33)}/quality_string.length
    end
    
    # values is an array of key (the name), flattened SAM fields
    # values are keyed on same "first" read position
    def finalize
      STDERR.puts "key is #{key.inspect}"
      if key =~ /^UNMAPPED/
        # unmapped reads, we just pass them on out
        values.each do |final|
          yield [
            # values.size, chunk.size,
            final[NAME_IDX+1],
            final[BIT_ONE_IDX+1], final[CHR_ONE_IDX+1], final[POS_ONE_IDX+1], final[SEQ_ONE_IDX+1], final[QUALITY_ONE_IDX+1],
            final[BIT_TWO_IDX+1], final[CHR_TWO_IDX+1], final[POS_TWO_IDX+1], final[SEQ_TWO_IDX+1], final[QUALITY_TWO_IDX+1]
          ]          
        end
        return
      end
      # we have the +1 on all the indexes since the values array has the key in the front
      values.group_by { |v| "#{v[RemoveLikePositionsFromFlattenedSAM::CHR_TWO_IDX+1]}_#{v[RemoveLikePositionsFromFlattenedSAM::POS_TWO_IDX+1]}"}.each do |key,chunk|
        # now we have a chunk of reads with all the same first position and all the same second position
        chunk.sort! do |a,b| 
          average_quality_score(a[QUALITY_ONE_IDX+1] + a[QUALITY_TWO_IDX+1]) <=> average_quality_score(b[QUALITY_ONE_IDX+1] + b[QUALITY_TWO_IDX+1])
        end
        final = chunk.first
        yield [
          # values.size, chunk.size,
          final[NAME_IDX+1],
          final[BIT_ONE_IDX+1], final[CHR_ONE_IDX+1], final[POS_ONE_IDX+1], final[SEQ_ONE_IDX+1], final[QUALITY_ONE_IDX+1],
          final[BIT_TWO_IDX+1], final[CHR_TWO_IDX+1], final[POS_TWO_IDX+1], final[SEQ_TWO_IDX+1], final[QUALITY_TWO_IDX+1]
        ]
      end
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  RemoveLikePositionsFromFlattenedSAM::Mapper,
  RemoveLikePositionsFromFlattenedSAM::Reducer
  ).run # Execute the script