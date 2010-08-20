require 'wukong'

require 'omrf/btangs/string_phred_extensions'

require 'omrf/btangs/cleaner/options_parse'
require 'omrf/btangs/cleaner/keys'
require 'omrf/btangs/read_status'
require 'omrf/btangs/cleaner/cleaner_mapper'
require 'omrf/btangs/cleaner/cleaner_reducer'

module OMRF
module Btangs
  module Cleaner
    
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

    JOINED_QSEQ_SEQUENCE_INDEX = 8
    JOINED_QSEQ_QUALITY_INDEX = 9
    JOINED_QSEQ_READ_END_INDEX = 7
    JOINED_QSEQ_SECOND_OFFSET = 11


    NO_QUALITY_SCORE = "B"
    NO_READ = "N"
  end
end
end