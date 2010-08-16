require 'omrf/btangs/cleaner'

module OMRF
module Btangs
module Cleaner
  class CleanerMapper < Wukong::Streamer::LineStreamer
    
    def initialize(*args)
      super(*args)
      if ("joined_pairs" == options[:key_type] || "sep_joined_pairs" == options[:key_type]) && !("joined_fastq" == options[:input_format] || "joined_qseq" == options[:input_format])
        raise "joined pairs must be used with joined_fastq or joined_qseq file format"
      end
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
      if key.kind_of?(Array)
        key.each do |k|
          yield [k, *parts]
        end
      else
        yield [key, *parts]
      end
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
        when /joined_qseq/i
          @quality_col = [OMRF::Btangs::Cleaner::JOINED_QSEQ_QUALITY_INDEX, OMRF::Btangs::Cleaner::JOINED_QSEQ_QUALITY_INDEX + OMRF::Btangs::Cleaner::JOINED_QSEQ_SECOND_OFFSET]
          [OMRF::Btangs::Cleaner::JOINED_QSEQ_SEQUENCE_INDEX, OMRF::Btangs::Cleaner::JOINED_QSEQ_SEQUENCE_INDEX + OMRF::Btangs::Cleaner::JOINED_QSEQ_SECOND_OFFSET]
        when /joined_fastq/i
          @quality_col = [ OMRF::Btangs::Cleaner::JOINED_FASTA_QUALITY_INDEX, OMRF::Btangs::Cleaner::JOINED_FASTA_QUALITY_INDEX+OMRF::Btangs::Cleaner::JOINED_FASTA_SECOND_OFFSET ]
          [OMRF::Btangs::Cleaner::JOINED_FASTA_SEQUENCE_INDEX, OMRF::Btangs::Cleaner::JOINED_FASTA_SEQUENCE_INDEX+OMRF::Btangs::Cleaner::JOINED_FASTA_SECOND_OFFSET]
        when /qseq/i
          @quality_col = OMRF::Btangs::Cleaner::QSEQ_QUALITY_INDEX
          OMRF::Btangs::Cleaner::QSEQ_SEQUENCE_INDEX
        when /fasta/i
          @quality_col = OMRF::Btangs::Cleaner::FASTA_QUALITY_INDEX
          OMRF::Btangs::Cleaner::FASTA_SEQUENCE_INDEX
        else
          nil
      end
    end
    
    def read_end_index
      @read_end_index ||=
      case sequence_index
        when OMRF::Btangs::Cleaner::QSEQ_SEQUENCE_INDEX
          OMRF::Btangs::Cleaner::QSEQ_READ_END_INDEX
        when OMRF::Btangs::Cleaner::FASTA_SEQUENCE_INDEX
          OMRF::Btangs::Cleaner::FASTA_READ_END_INDEX
        when [OMRF::Btangs::Cleaner::JOINED_FASTA_SEQUENCE_INDEX, OMRF::Btangs::Cleaner::JOINED_FASTA_SEQUENCE_INDEX+OMRF::Btangs::Cleaner::JOINED_FASTA_SECOND_OFFSET]
          [OMRF::Btangs::Cleaner::JOINED_FASTA_SEQUENCE_INDEX, OMRF::Btangs::Cleaner::JOINED_FASTA_SEQUENCE_INDEX + OMRF::Btangs::Cleaner::JOINED_FASTA_SECOND_OFFSET]
        when [OMRF::Btangs::Cleaner::JOINED_QSEQ_SEQUENCE_INDEX, OMRF::Btangs::Cleaner::JOINED_QSEQ_SEQUENCE_INDEX+OMRF::Btangs::Cleaner::JOINED_QSEQ_SECOND_OFFSET]
          [OMRF::Btangs::Cleaner::JOINED_QSEQ_SEQUENCE_INDEX, OMRF::Btangs::Cleaner::JOINED_QSEQ_SEQUENCE_INDEX + OMRF::Btangs::Cleaner::JOINED_QSEQ_SECOND_OFFSET]
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
    
    def joined_pairs_both_key(parts)
      key = []
      @sequence_index.each_with_index do |seq_index, read_no|
        sequence = parts[seq_index]
        key << sequence[@key_range]
        key << (sequence.reverse)[@key_range].reverse
      end
      return key.join("_")
    end
    
    def single_end_joined_pairs_both_key(parts)
      key = []
      @sequence_index.each_with_index do |seq_index, read_no|
        sequence = parts[seq_index]
        key << "#{sequence[@key_range]}_#{(sequence.reverse)[@key_range].reverse}"
      end
      return key
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
        when /sep_joined_pairs/i
          read_end_index()
          if options[:both_ends] then
            alias line_key single_end_joined_pairs_both_key
          else
            alias line_key joined_pairs_single_end_key
          end
        when /joined_pairs/i
          read_end_index()
          if options[:both_ends] then
            alias line_key joined_pairs_both_key
          else
            alias line_key joined_pairs_single_end_key
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
  end #CleanerMapper
end #cleaner
end #Btangs
end #ORMF