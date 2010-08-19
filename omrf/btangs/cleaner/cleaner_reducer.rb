require 'omrf/btangs/cleaner'

module OMRF
module Btangs
module Cleaner
  class CleanerReducer < Wukong::Streamer::ListReducer
    
    def initialize(*args)
      super(*args)
      parse_format()

      if "joined_fastq" == options[:input_format] || "joined_qseq" == options[:input_format]
        alias part_for_comparison all_sequence_for_comparison_pair
      else
        alias part_for_comparison all_sequence_for_comparison
      end

      key_range()
    end
    
    def parse_format()
      alias quality_for_read quality_for_read_single
      case options[:input_format]
        when /joined_fastq/i
          @sequence_col = [OMRF::Btangs::Cleaner::JOINED_FASTA_SEQUENCE_INDEX, OMRF::Btangs::Cleaner::JOINED_FASTA_SEQUENCE_INDEX + OMRF::Btangs::Cleaner::JOINED_FASTA_SECOND_OFFSET]
          @quality_col = [OMRF::Btangs::Cleaner::JOINED_FASTA_QUALITY_INDEX, OMRF::Btangs::Cleaner::JOINED_FASTA_QUALITY_INDEX + OMRF::Btangs::Cleaner::JOINED_FASTA_SECOND_OFFSET]
          alias quality_for_read quality_for_read_pair
        when /joined_qseq/i
          @sequence_col = [OMRF::Btangs::Cleaner::JOINED_QSEQ_SEQUENCE_INDEX, OMRF::Btangs::Cleaner::JOINED_QSEQ_SEQUENCE_INDEX + OMRF::Btangs::Cleaner::JOINED_QSEQ_SECOND_OFFSET]
          @quality_col = [OMRF::Btangs::Cleaner::JOINED_QSEQ_QUALITY_INDEX, OMRF::Btangs::Cleaner::JOINED_QSEQ_QUALITY_INDEX + OMRF::Btangs::Cleaner::JOINED_QSEQ_SECOND_OFFSET]
          alias quality_for_read quality_for_read_pair
        when /qseq/i
          @sequence_col = OMRF::Btangs::Cleaner::QSEQ_SEQUENCE_INDEX
          @quality_col = OMRF::Btangs::Cleaner::QSEQ_QUALITY_INDEX
        when /fasta/i
          @sequence_col = OMRF::Btangs::Cleaner::FASTA_SEQUENCE_INDEX
          @quality_col = OMRF::Btangs::Cleaner::FASTA_QUALITY_INDEX
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
    def top_quality_index(qualities)
      max = -100.0
      index = 0
      qualities.each_with_index do |qual,i|
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
    
    
    def all_sequence_for_comparison(parts)
      parts[@sequence_col]
    end
    
    def all_sequence_for_comparison_pair(parts)
      @sequence_col.map {|sc| parts[sc]}.join("")
    end
    
    def joined_pairs_both_key(parts)
      key = []
      @sequence_col.each_with_index do |seq_index, read_no|
        sequence = parts[seq_index]
        key << sequence[@key_range]
        key << (sequence.reverse)[@key_range].reverse
      end
      key
    end
    
    def matches_best(read,best)
      compare_keys = joined_pairs_both_key(read)
      
      return ( # A1+A2 == B1+B2
               best[0] == compare_keys[0] && best[1] == compare_keys[1] &&
               best[2] == compare_keys[2] && best[3] == compare_keys[3]
             ) ||
             ( # A1+A2 == B2+B1
               best[0] == compare_keys[2] && best[1] == compare_keys[3] &&
               best[2] == compare_keys[0] && best[3] == compare_keys[1]
             )
    end
    
    def name_for(read)
      case options[:input_format]
        when /fast/i
          read[0].split(/\//).first #strip of the end /read_number
        when /qseq/i
          read[0..6].join("_")
      end
    end
    
    # values is an array of key, input format fields
    def finalize
      # strip the first element, since is the key
      values.map {|v| v.shift }

      values.sort! do |a,b| 
        s = name_for(a) <=> name_for(b)
        if 0 == s
          part_for_comparison(a) <=> part_for_comparison(b)
        else
          s
        end
      end
      
      h_key = values.first.first

      best_index = top_quality_index(values)
      best = values.delete_at(best_index)
      best_sequence = part_for_comparison(best)
      
      best_keys = joined_pairs_both_key(best)
      best_name = name_for(best)
      
      best_for = 0
      values.each do |v|
        if matches_best(v,best_keys) then
          yield [ v, "REJECT", best_name] if options[:include_rejects]
          best_for += 1
        else   
          yield [ v, "PASS_DIDNT_MATCH", best_name ]
        end
      end #values
      
      best_msg = if best_for > 0 then
        ["PASS_BEST_FOR", best_for]
      else
        ["PASS_ONLY","ALONE"]
      end
      yield [ best + best_msg ]

    end #finalize
    
  end #Cleanerreducer
end #Cleaner
end #Btangs
end #omrf