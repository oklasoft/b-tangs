require 'omrf/btangs/cleaner'

module OMRF
module Btangs
module Cleaner
  class CleanerMapper < Wukong::Streamer::LineStreamer
    
    include OMRF::Btangs::Cleaner::OptionsParse
    
    def initialize(*args)
      super(*args)
      check_key_type_works_for_input_format!()
      parse_format()
      parse_key_range()
      parse_key_type()
    end

    #
    # For each line make the key(s) and emit them then for that line
    #
    def process(line)
      parts = line.chomp.split(/\t/)
      keys = line_key(parts)
      keys.each do |k|
        yield [k, *parts]
      end
    end #process
    
    def single_end_keys(parts)
      key = []
      @sequence_index.each_with_index do |seq_index, read_no|
        sequence = parts[seq_index]
        key << "#{sequence[@key_range]}"
      end
      return key
    end

    def both_ends_keys(parts)
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
          if options[:both_ends] then
            alias line_key both_ends_keys
          else
            # TODO
            alias line_key single_end_keys
          end
        when /single/i
          if options[:both_ends] then
            alias line_key both_ends_keys
          else
            alias line_key single_end_keys
          end
        else
          raise "Please specify type of key --key_type (sep_joined_pairs, single)"
      end
    end
    
  end #CleanerMapper
end #cleaner
end #Btangs
end #ORMF