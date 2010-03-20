#!/usr/bin/ruby1.9
require 'wukong'
require 'amatch'

module SequenceBinner
  
  class Mapper < Wukong::Streamer::LineStreamer
    
    #
    # lzop -dc 101292s_1_1_export.txt.lzo| awk -F '\t' '{print $1":"NR"\t"$9"\t"$10}' > 1.txt
    # lzop -dc lgs101435_s_1_1_qseq_raw.txt.lzo | egrep '1$' | awk -F '\t' '{print $1":"$2":"$3":"$4":"$5":"$6":"$7":"$8"\t"$9"\t"$10}' > 1.txt
    #
    def process line
      # (line_number_forward,reader_forward,sequence_forward,quality_forward,
      #  line_number_reverse,reader_reverse,sequence_reverse,quality_reverse) = line.split(/\t/)
      #  read_name = "@#{reader_reverse}:#{line_number_forward}"
      (read_name,sequence_forward,quality_forward,
       sequence_reverse,quality_reverse) = line.split(/\t/)      
      yield [sequence_forward[key_range], read_name, sequence_forward, quality_forward, sequence_reverse, quality_reverse]
    end
    
    def key_range
      @key_range ||= parse_key_range(options[:range_start],options[:range_size]) or
        raise "Please supply both a --range_start= and --range_size= argument"
    end
    
    # given a start position and a length create a valid range
    def parse_key_range(start,length)
      return nil unless options[:range_start] && options[:range_size]
      Range.new(start.to_i, start.to_i+length.to_i,true)
    end
  end

  class Reducer < Wukong::Streamer::ListReducer
    
    KEY_COL = 0
    NAME_COL = 1
    SEQUENCE_COL = 2
    QUALITY_COL = 3
    SEQUENCE_COL_REV = 4
    QUALITY_COL_REV = 5
    
    def phred_quality(char)
      char.ord - 33
    end
    
    def phred_quality_sum(quality)
      quality.each_char.inject(0.0) {|sum,char| sum += phred_quality(char)}
    end

    def top_quality_index(qualities)
      max = -100.0
      index = 0
      qualities.each_with_index do |qual,i|
        sum = phred_quality_sum(qual[QUALITY_COL])
        if  sum > max then
          max = sum
          index = i
        end
      end
      index
    end
    
    # values is an array of key, read_name, sequence, quality
    def finalize
      best_index = top_quality_index(values)
      best = values.delete_at(best_index)
      best_sequence = best[SEQUENCE_COL]
      yield [ best[NAME_COL], best_sequence, best[QUALITY_COL], best[SEQUENCE_COL_REV], best[QUALITY_COL_REV] ]
      levenshtein_pattern = Amatch::Levenshtein.new(best_sequence)
      values.each do |v|
        if best_sequence == v[SEQUENCE_COL] || levenshtein_pattern.similar(v[SEQUENCE_COL]) >= 0.90 then
          next
        end
        yield [ v[NAME_COL], v[SEQUENCE_COL], v[QUALITY_COL], v[SEQUENCE_COL_REV], v[QUALITY_COL_REV] ]
      end #values
      
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  SequenceBinner::Mapper,
  SequenceBinner::Reducer
  ).run # Execute the script