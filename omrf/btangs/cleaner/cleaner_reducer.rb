require 'omrf/btangs/cleaner'

module OMRF
module Btangs
module Cleaner
  class CleanerReducer < Wukong::Streamer::ListReducer
    
    include OMRF::Btangs::Cleaner::OptionsParse
    include OMRF::Btangs::Cleaner::Keys
    
    def initialize(*args)
      super(*args)
      parse_format()
      parse_key_range()
      parse_key_type()
    end

    def quality_for_read(read)
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

    def part_for_comparison(parts)
      @sequence_index.map {|sc| parts[sc]}.join("")
    end
    
    def matches_best(read,best)
      compare_keys = key_parts(read)

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
      if INPUT_FORMATS[:fastq] == options[:input_format] || INPUT_FORMATS[:joined_fastq] == options[:input_format] 
        read[0].split(/\//).first #strip of the end /read_number
      elsif INPUT_FORMATS[:fastq18] == options[:input_format] || INPUT_FORMATS[:joined_fastq18] == options[:input_format] 
        read[0].split(/\s/).first #strip of the end /read_number
      elsif INPUT_FORMATS[:qseq] == options[:input_format] || INPUT_FORMATS[:joined_qseq] == options[:input_format]
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
      
      best_keys = key_parts(best)
      best_name = name_for(best)
      
      best_for = 0
      values.each do |v|
        if matches_best(v,best_keys) then
          yield [ v, ReadStatus::REJECT, best_name] if options[:include_rejects]
          best_for += 1
        else   
          yield [ v, ReadStatus::PASS_DIDNT_MATCH, best_name ]
        end
      end #values
      
      best_msg = if best_for > 0 then
        [ReadStatus::PASS_BEST_FOR, best_for]
      else
        [ReadStatus::PASS_ONLY,"ALONE"]
      end
      yield [ best + best_msg ]

    end #finalize
    
  end #Cleanerreducer
end #Cleaner
end #Btangs
end #omrf
