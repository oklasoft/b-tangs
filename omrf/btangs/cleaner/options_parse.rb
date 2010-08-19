module OMRF
module Btangs
module Cleaner
  module OptionsParse
    def key_range
      @key_range ||= parse_key_range(options[:range_start],options[:range_size]) or
        raise "Please supply both a --range_start= and --range_size= argument"
    end
  end
end
end #btangs
end #omrf