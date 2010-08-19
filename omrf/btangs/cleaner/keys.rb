module OMRF
module Btangs
module Cleaner
  module Keys
    
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
    
  end
end #Cleaner
end #Btangs
end #OMRF
