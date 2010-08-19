module OMRF
module Btangs
module Cleaner
  module OptionsParse

    INPUT_FORMATS =
    {
      :joined_fastq => "joined_fastq",
      :joined_qseq => "joined_qseq",
      :qseq => "qseq",
      :fastq => "fastq"
    }
    
    def check_key_type_works_for_input_format!()
      if ("sep_joined_pairs" == options[:key_type]) && !(INPUT_FORMATS[:joined_fastq] == options[:input_format] || INPUT_FORMATS[:joined_qseq] == options[:input_format])
        raise "joined pairs must be used with #{INPUT_FORMATS[:joined_qseq]} or #{INPUT_FORMATS[:joined_fastq]} file format"
      end
    end
    
    def parse_key_range
      unless options[:range_start] && options[:range_size]
        raise "Please supply both a --range_start= and --range_size= argument"
      end
      @key_range ||= Range.new(options[:range_start].to_i, options[:range_start].to_i+options[:range_size].to_i,true)
    end
    
    def parse_format()
      case options[:input_format]
        when INPUT_FORMATS[:joined_fastq]
          @sequence_index = [OMRF::Btangs::Cleaner::JOINED_FASTA_SEQUENCE_INDEX, OMRF::Btangs::Cleaner::JOINED_FASTA_SEQUENCE_INDEX + OMRF::Btangs::Cleaner::JOINED_FASTA_SECOND_OFFSET]
          @quality_col = [OMRF::Btangs::Cleaner::JOINED_FASTA_QUALITY_INDEX, OMRF::Btangs::Cleaner::JOINED_FASTA_QUALITY_INDEX + OMRF::Btangs::Cleaner::JOINED_FASTA_SECOND_OFFSET]
        when INPUT_FORMATS[:joined_qseq]
          @sequence_index = [OMRF::Btangs::Cleaner::JOINED_QSEQ_SEQUENCE_INDEX, OMRF::Btangs::Cleaner::JOINED_QSEQ_SEQUENCE_INDEX + OMRF::Btangs::Cleaner::JOINED_QSEQ_SECOND_OFFSET]
          @quality_col = [OMRF::Btangs::Cleaner::JOINED_QSEQ_QUALITY_INDEX, OMRF::Btangs::Cleaner::JOINED_QSEQ_QUALITY_INDEX + OMRF::Btangs::Cleaner::JOINED_QSEQ_SECOND_OFFSET]
        when INPUT_FORMATS[:qseq]
          @sequence_index = [OMRF::Btangs::Cleaner::QSEQ_SEQUENCE_INDEX]
          @quality_col = [OMRF::Btangs::Cleaner::QSEQ_QUALITY_INDEX]
        when INPUT_FORMATS[:fastq]
          @sequence_index = [OMRF::Btangs::Cleaner::FASTA_SEQUENCE_INDEX]
          @quality_col = [OMRF::Btangs::Cleaner::FASTA_QUALITY_INDEX]
        else
          raise "Please let us know the input file format with --input_format= argument"
      end
    end
    
  end
end
end #btangs
end #omrf