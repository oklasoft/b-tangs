require 'omrf/btangs/cleaner'

module OMRF
module Btangs
module Cleaner
  class CleanerMapper < Wukong::Streamer::LineStreamer
    
    def initialize(*args)
      super(*args)
      if ("sep_joined_pairs" == options[:key_type]) && !("joined_fastq" == options[:input_format] || "joined_qseq" == options[:input_format])
        raise "joined pairs must be used with joined_fastq or joined_qseq file format"
      end
      sequence_index()
      key_range()
      parse_key_type()
    end
    
    # given a start position and a length create a valid range
    def parse_key_range(start,length)
      return nil unless options[:range_start] && options[:range_size]
      Range.new(start.to_i, start.to_i+length.to_i,true)
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
    
    def sequence_index
      @sequence_index ||= parse_format(options[:input_format]) or
        raise "Please let us know the input file format with --input_format= argument"
    end
    
    
    #
    # For each line make the key(s) and emit them then for that line
    #
    def process(line)
      parts = line.chomp.split(/\t/)

      key = line_key(parts)
      return unless key

      if key.kind_of?(Array)
        key.each do |k|
          yield [k, *parts]
        end
      else
        yield [key, *parts]
      end
    end #process
    
    def single_end_key(parts)
      parts[@sequence_index][@key_range]
    end
    
    def single_end_both_key(parts)
      sequence = parts[@sequence_index]
      front = sequence[@key_range]
      back = (sequence.reverse)[@key_range].reverse
      key = "#{front}_#{back}"
      key
    end
    
    def single_end_joined_pairs_both_key(parts)
      key = []
      @sequence_index.each_with_index do |seq_index, read_no|
        sequence = parts[seq_index]
        key << "#{sequence[@key_range]}_#{(sequence.reverse)[@key_range].reverse}"
      end
      return key
    end
    
    def parse_key_type()
      case options[:key_type]
        when /sep_joined_pairs/i
          read_end_index()
          if options[:both_ends] then
            alias line_key single_end_joined_pairs_both_key
          else
            # TODO
          end
        when /single/i
          if options[:both_ends] then
            alias line_key single_end_both_key
          else
            alias line_key single_end_key
          end
        else
          raise "Please specify type of key --key_type (paired, single, acgt_avg)"
      end
      @key_range ||= parse_key_range(options[:range_start],options[:range_size]) or
        raise "Please supply both a --range_start= and --range_size= argument"
    end
    
  end #CleanerMapper
end #cleaner
end #Btangs
end #ORMF