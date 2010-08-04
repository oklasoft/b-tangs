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
  
  JOINED_FASTA_SEQUENCE_INDEX = 1
  JOINED_FASTA_QUALITY_INDEX = 3
  JOINED_FASTA_READ_END_INDEX = 4
  JOINED_FASTA_SECOND_OFFSET = 5
  
  NO_QUALITY_SCORE = "B"
  NO_READ = "N"
  
  class Mapper < Wukong::Streamer::LineStreamer
    
    def initialize(*args)
      super(*args)
      options[:key_type] = "joined_fastq" if "joined_fastq" == options[:input_format]
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
        when /joined_fastq/i
          @quality_col = [ SequenceBinner::JOINED_FASTA_QUALITY_INDEX, SequenceBinner::JOINED_FASTA_QUALITY_INDEX+SequenceBinner::JOINED_FASTA_SECOND_OFFSET ]
          [SequenceBinner::JOINED_FASTA_SEQUENCE_INDEX, SequenceBinner::JOINED_FASTA_SEQUENCE_INDEX+SequenceBinner::JOINED_FASTA_SECOND_OFFSET]
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
        when [SequenceBinner::JOINED_FASTA_SEQUENCE_INDEX, SequenceBinner::JOINED_FASTA_SEQUENCE_INDEX+SequenceBinner::JOINED_FASTA_SECOND_OFFSET]
          [SequenceBinner::JOINED_FASTA_SEQUENCE_INDEX, SequenceBinner::JOINED_FASTA_SEQUENCE_INDEX + SequenceBinner::JOINED_FASTA_SECOND_OFFSET]
      end
    end
    
    def key_range
      @key_range ||= parse_key_range(options[:range_start],options[:range_size]) or
        raise "Please supply both a --range_start= and --range_size= argument"
    end
    
    def acgt_averages(seq)
      sums = {"A" => 0,"C" => 0,"G" => 0,"T" => 0}
      seq.chars.each {|c| sums[c] += 1 if sums.keys.include?(c)}
      sums.each {|l,i| sums[l] = (i/seq.length.to_f*100).floor}
      sums
    end

    def compress(term)
      Zlib::Deflate.deflate(term.strip.downcase)
    end

    def zlib_key(term)
      n = "N" * term.size
      key = (compress(n+term).size - compress(n).size)/term.size.to_f
      (key * 100).to_i
    end

    def single_end_key(parts)
      parts[@sequence_index][@key_range]
    end
    
    def single_end_zlib_key(parts)
      zlib_key(parts[@sequence_index][@key_range])
    end
    
    def single_end_both_key(parts)
      sequence = parts[@sequence_index]
      front = sequence[@key_range]
      back = (sequence.reverse)[@key_range].reverse
      key = "#{front}_#{back}"
      return "#{key}_possiblepcr" if front == back && options[:possible_pcr]
      key
    end
    
    def acgt_key(parts)
      sequence = parts[@sequence_index]
      front = sequence[@key_range]
      avgs = acgt_averages(front)
      key = []
      %w/A C G T/.each {|b| key << "#{b}:#{avgs[b]}"}
      key.join("_")
    end

    def acgt_both_ends_key(parts)
      sequence = parts[@sequence_index]
      front = sequence[@key_range]
      back = (sequence.reverse)[@key_range].reverse
      avgs = acgt_averages(front+back)
      key = []
      %w/A C G T/.each {|b| key << "#{b}:#{avgs[b]}"}
      key.join("_")
    end
    
    def paired_end_key(parts)
      "#{parts[@read_end_index]}_#{single_end_key(parts)}"
    end

    def paired_end_both_key(parts)
      single_key = single_end_both_key(parts)
      return nil if nil == single_key
      "#{parts[@read_end_index]}_#{single_key}"
    end
    
    def joined_fastq_single_end_both_key(parts)
      key = []
      @sequence_index.each_with_index do |seq_index, read_no|
        sequence = parts[seq_index]
        key << sequence[@key_range]
        key << (sequence.reverse)[@key_range].reverse
      end
      return key.join("_")
    end
    
    def parse_endness_key
      case options[:key_type]
        when /acgt_avg/i
          if options[:both_ends] then
            alias line_key acgt_both_ends_key
          else
            alias line_key acgt_key
          end
        when /paired/i
          read_end_index()
          if options[:both_ends] then
            alias line_key paired_end_both_key
          else
            alias line_key paired_end_key
          end
        when /joined_fastq/i
          read_end_index()
          if options[:both_ends] then
            alias line_key joined_fastq_single_end_both_key
          else
            alias line_key joined_fastq_single_end_key
          end
        when /single/i
          if options[:both_ends] then
            alias line_key single_end_both_key
          else
            alias line_key single_end_key
          end
        when /zlib/i
          require 'zlib'
          if options[:both_ends] then
            alias line_key single_end_zlib_both_key
          else
            alias line_key single_end_zlib_key
          end
        else
          raise "Please specify type of key --key_type (paired, single, acgt_avg)"
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
      case options[:key_type]
        when /acgt_avg/i
          alias part_for_comparison sequence_tips_for_comparison
        else
          if "joined_fastq" == options[:input_format] 
            alias part_for_comparison all_sequence_for_comparison_pair
          else
            alias part_for_comparison all_sequence_for_comparison
          end
      end
      key_range()
    end
    
    def parse_format()
      alias quality_for_read quality_for_read_single
      case options[:input_format]
        when /qseq/i
          @sequence_col = SequenceBinner::QSEQ_SEQUENCE_INDEX
          @quality_col = SequenceBinner::QSEQ_QUALITY_INDEX
        when /fasta/i
          @sequence_col = SequenceBinner::FASTA_SEQUENCE_INDEX
          @quality_col = SequenceBinner::FASTA_QUALITY_INDEX
        when /joined_fastq/i
          @sequence_col = [SequenceBinner::JOINED_FASTA_SEQUENCE_INDEX, SequenceBinner::JOINED_FASTA_SEQUENCE_INDEX + SequenceBinner::JOINED_FASTA_SECOND_OFFSET]
          @quality_col = [SequenceBinner::JOINED_FASTA_QUALITY_INDEX, SequenceBinner::JOINED_FASTA_QUALITY_INDEX + SequenceBinner::JOINED_FASTA_SECOND_OFFSET]
          alias quality_for_read quality_for_read_pair
        else
          raise "Please let us know the input file format with --input_format= argument"
      end
    end
    
    def quality_for_read_single(read)
      read[@quality_col]
    end

    def quality_for_read_pair(read)
      @quality_col.map {|q| read[q]}.join("")
    end

    # find the best score in this set
    # ALSO we shift off the first element of each value array, thus removing
    # the key column from all, so our indexes picked above actually work
    def top_quality_index!(qualities)
      max = -100.0
      index = 0
      qualities.each_with_index do |qual,i|
        qual.shift
        avg = quality_for_read(qual).phred_quality_score_average
        if  avg > max then
          max = avg
          index = i
        end
      end
      index
    end
    
    def key_range
      @key_range ||= parse_key_range(options[:range_start],options[:range_size]) or
        raise "Please supply both a --range_start= and --range_size= argument"
    end
    
    def parse_key_range(start,length)
      return nil unless options[:range_start] && options[:range_size]
      Range.new(start.to_i, start.to_i+length.to_i,true)
    end
    
    
    def sequence_tips_for_comparison(parts)
      sequence = parts[@sequence_col]
      front = sequence[@key_range]
      back = ""
      back = (sequence.reverse)[@key_range].reverse if options[:both_ends]
      front + back
    end
    
    def all_sequence_for_comparison(parts)
      parts[@sequence_col]
    end
    
    def all_sequence_for_comparison_pair(parts)
      @sequence_col.map {|sc| parts[sc]}.join("")
    end
    
    # values is an array of key, input format fields
    def finalize
      reject_all_but_top = false
      if key =~ /_possiblepcr/
        reject_all_but_top = true unless (options[:trim_pcr_quality] || options[:trim_pcr_read])
      end

      best_index = top_quality_index!(values)
      best = values.delete_at(best_index)
      best_sequence = part_for_comparison(best)
      
      yield [ best + ["PASS"] ]
      if 0.0 == @similarity
        reject_all_but_top = true
        # return
      end
      levenshtein_pattern = Amatch::Levenshtein.new(best_sequence)
      values.each do |v|
        if reject_all_but_top || levenshtein_pattern.similar(part_for_comparison(v)) >= @similarity then
          yield [ v + ["REJECT #{best[0]}"]]
          next
        end        
        yield [ v + ["PASS"] ]
      end #values

    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  SequenceBinner::Mapper,
  SequenceBinner::Reducer
  ).run # Execute the script