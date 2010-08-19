require 'omrf/btangs/cleaner'

module OMRF
module Btangs
module Cleaner
  class CleanerMapper < Wukong::Streamer::LineStreamer
    
    include OMRF::Btangs::Cleaner::OptionsParse
    include OMRF::Btangs::Cleaner::Keys
    
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
    
    
  end #CleanerMapper
end #cleaner
end #Btangs
end #ORMF