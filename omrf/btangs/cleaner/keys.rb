module OMRF
module Btangs
module Cleaner
  module Keys
    
    def front_end_key_parts(line_parts)
      key = []
      @sequence_index.each_with_index do |seq_index, read_no|
        sequence = line_parts[seq_index]
        key << sequence[@key_range]
      end
      key
    end
    
    def front_end_keys(parts)
      key_parts = front_end_key_parts(parts)
      return key_parts
    end

    def both_end_key_parts(line_parts)
      key = []
      @sequence_index.each_with_index do |seq_index, read_no|
        sequence = line_parts[seq_index]
        key << sequence[@key_range]
        key << (sequence.reverse)[@key_range].reverse
      end
      key
    end

    def both_ends_keys(parts)
      key_parts = both_end_key_parts(parts)
      return key_parts.each_slice(2).inject([]) {|a,e| a << e.join("_")}
    end
    
    def parse_key_type()
      case options[:key_type]
        when /sep_joined_pairs/i
          if options[:both_ends] then
            alias line_key both_ends_keys
            alias key_parts both_end_key_parts
          else
            alias line_key front_end_keys
            alias key_parts front_end_key_parts
          end
        when /single/i
          if options[:both_ends] then
            alias line_key both_ends_keys
            alias key_parts both_end_key_parts
          else
            alias line_key front_end_keys
            alias key_parts front_end_key_parts
          end
        else
          raise "Please specify type of key --key_type (sep_joined_pairs, single)"
      end
    end
    
  end
end #Cleaner
end #Btangs
end #OMRF
