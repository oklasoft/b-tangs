#!/usr/bin/ruby1.9
require 'wukong'
require 'amatch'

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

module SequenceBinner
  FASTA_SEQUENCE_INDEX = 1
  FASTA_QUALITY_INDEX = 3
  FASTA_READ_END_INDEX = 4
  QSEQ_SEQUENCE_INDEX = 8
  QSEQ_QUALITY_INDEX = 9
  QSEQ_READ_END_INDEX = 7
  
  NO_QUALITY_SCORE = "B"
  NO_READ = "N"
  
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
      key = line_key(parts)
      return unless key
      if key =~ /_possiblepcr/
        trim_ends_by_resetting_quality_score!(parts) if options[:trim_pcr_quality]
        trim_ends_by_resetting_reading!(parts) if options[:trim_pcr_read]
      end
      yield [key, *parts]
    end
    
    def trim_ends_by_resetting_quality_score!(parts)
      new_quality = NO_QUALITY_SCORE * options[:range_size].to_i
      parts[@quality_col][@key_range] = new_quality
      (parts[@quality_col].reverse!)[@key_range] = new_quality
      parts[@quality_col].reverse!
    end

    def trim_ends_by_resetting_reading!(parts)
      new_quality = NO_READ * options[:range_size].to_i
      parts[@sequence_index][@key_range] = new_quality
      (parts[@sequence_index].reverse!)[@key_range] = new_quality
      parts[@sequence_index].reverse!
    end
    
    def sequence_index
      @sequence_index ||= parse_format(options[:input_format]) or
        raise "Please let us know the input file format with --input_format= argument"
    end
    
    def parse_format(input_format)
      case input_format
        when /qseq/i
          @quality_col = SequenceBinner::QSEQ_QUALITY_INDEX
          SequenceBinner::QSEQ_SEQUENCE_INDEX
        when /fasta/i
          @quality_col = SequenceBinner::FASTA_QUALITY_INDEX
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
    
    def single_end_both_key(parts)
      sequence = parts[@sequence_index]
      front = sequence[@key_range]
      back = (sequence.reverse)[@key_range].reverse
      key = "#{front}_#{back}"
      return "#{key}_possiblepcr" if front == back
      key
    end
    
    def paired_end_key(parts)
      "#{parts[@read_end_index]}_#{single_end_key(parts)}"
    end

    def paired_end_both_key(parts)
      single_key = single_end_both_key(parts)
      return nil if nil == single_key
      "#{parts[@read_end_index]}_#{single_key}"
    end
    
    def parse_endness_key
      case options[:end_style]
        when /paired/i
          read_end_index()
          if options[:both_ends] then
            alias line_key paired_end_both_key
          else
            alias line_key paired_end_key
          end
        when /single/i
          if options[:both_ends] then
            alias line_key single_end_both_key
          else
            alias line_key single_end_key
          end
        else
          raise "Please specify paired or single with --end_style"
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
        avg = qual[@quality_col].phred_quality_score_average
        if  avg > max then
          max = avg
          index = i
        end
      end
      index
    end
    
    # values is an array of key, input format fields
    def finalize
      reject_all_but_top = false
      if key =~ /_possiblepcr/
        reject_all_but_top = true unless (options[:trim_pcr_quality] || options[:trim_pcr_read])
      end
      
      best_index = top_quality_index!(values)
      best = values.delete_at(best_index)
      best_sequence = best[@sequence_col]
      yield [ best ]
      if 0.0 == @similarity
        return
      end
      levenshtein_pattern = Amatch::Levenshtein.new(best_sequence)
      values.each do |v|
        if reject_all_but_top || levenshtein_pattern.similar(v[@sequence_col]) >= @similarity then
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