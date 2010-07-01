#!/usr/bin/ruby1.9
require 'wukong'
require 'amatch'

class String
  
  # sam : ASCII - 33
  # illuma fastq : ASCII - 64
  def phred_quality_score_sum
    self.each_char.inject(0.0) {|sum,char| sum += (char.ord-64)}
  end
end

module SequenceBinner
  FASTA_SEQUENCE_INDEX = 1
  FASTA_QUALITY_INDEX = 3
  FASTA_READ_END_INDEX = 4
  QSEQ_SEQUENCE_INDEX = 8
  QSEQ_QUALITY_INDEX = 9
  QSEQ_READ_END_INDEX = 7
  
  class Mapper < Wukong::Streamer::LineStreamer
    
    def initialize(*args)
      super(*args)
      sequence_index()
      key_range()
      parse_endness_key()
    end
    
    #
    # lzop -dc 101292s_1_1_export.txt.lzo| awk -F '\t' '{print $1":"NR"\t"$9"\t"$10}' > 1.txt
    # lzop -dc lgs101435_s_1_1_qseq_raw.txt.lzo | egrep '1$' | awk -F '\t' '{print $1":"$2":"$3":"$4":"$5":"$6":"$7":"$8"\t"$9"\t"$10}' > 1.txt
    # awk -F '\t' '2==$8 {print $0}' > 2.txt # for final output
    #
    def process line
      parts = line.chomp.split(/\t/)      
      yield [line_key(parts), *parts]
    end
    
    def sequence_index
      @sequence_index ||= parse_format(options[:input_format]) or
        raise "Please let us know the input file format with --input_format= argument"
    end
    
    def parse_format(input_format)
      case input_format
        when /qseq/i
          SequenceBinner::QSEQ_SEQUENCE_INDEX
        when /fasta/i
          SequenceBinner::FASTA_SEQUENCE_INDEX
        else
          nil
      end
    end
    
    def read_end_index
      @read_end_index ||=
      case sequence_index
        when SequenceBinner::QSEQ_SEQUENCE_INDEX
          SequenceBinner::QSEQ_READ_END_INDEX
        when SequenceBinner::FASTA_SEQUENCE_INDEX
          SequenceBinner::FASTA_READ_END_INDEX
      end
    end
    
    def key_range
      @key_range ||= parse_key_range(options[:range_start],options[:range_size]) or
        raise "Please supply both a --range_start= and --range_size= argument"
    end

    def single_end_key(parts)
      parts[@sequence_index][@key_range]
    end
    
    def paired_end_key(parts)
      "#{parts[@read_end_index]}_#{single_end_key(parts)}"
    end
    
    def parse_endness_key
      case options[:endedness]
        when /paired/i
          read_end_index()
          alias line_key paired_end_key
        when /single/i
          alias line_key single_end_key
        else
          raise "Please specify paired or single with --endedness"
      end
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
    
    def initialize(*args)
      super(*args)
      parse_format()
      @similarity ||= (options[:similarity].to_f || 0.90)
    end
    
    def parse_format()
      case options[:input_format]
        when /qseq/i
          @sequence_col = SequenceBinner::QSEQ_SEQUENCE_INDEX
          @quality_col = SequenceBinner::QSEQ_QUALITY_INDEX
        when /fasta/i
          @sequence_col = SequenceBinner::FASTA_SEQUENCE_INDEX
          @quality_col = SequenceBinner::FASTA_QUALITY_INDEX
        else
          raise "Please let us know the input file format with --input_format= argument"
      end
    end

    # find the best score in this set
    # ALSO we shift off the first element of each value array, thus removing
    # the key column from all, so our indexes picked above actually work
    def top_quality_index!(qualities)
      max = -100.0
      index = 0
      qualities.each_with_index do |qual,i|
        qual.shift
        sum = qual[@quality_col].phred_quality_score_sum
        if  sum > max then
          max = sum
          index = i
        end
      end
      index
    end
    
    # values is an array of key, input format fields
    def finalize
      best_index = top_quality_index!(values)
      best = values.delete_at(best_index)
      best_sequence = best[@sequence_col]
      yield [ best ]
      levenshtein_pattern = Amatch::Levenshtein.new(best_sequence)
      values.each do |v|
        if best_sequence == v[@sequence_col] || levenshtein_pattern.similar(v[@sequence_col]) >= @similarity then
          next
        end
        yield [ v ]
      end #values
      
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  SequenceBinner::Mapper,
  SequenceBinner::Reducer
  ).run # Execute the script