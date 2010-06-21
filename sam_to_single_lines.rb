#!/usr/bin/ruby1.9
require 'wukong'

# Given a SAM file, make up something new where the pairs are together one one
# line. Also we only care about the name, sequence, quality, bit flags, and maps
module SamToSingleLines

  class Mapper < Wukong::Streamer::LineStreamer

    # input is a line from a SAM file, we are only concerned with non headaers
    def process line
      return if line =~ /^@/
      parts = line.chomp.split(/\t/)
      yield [parts.shift, *parts]
    end

  end

  class Reducer < Wukong::Streamer::ListReducer

    # values is an array of key (the name), SAM fields (minues name)
    # we'll get now a line with the name, bit, chr, pos, seq, qual, bit, chr, pos, seq, qual
    def finalize
      return unless 2 == values.size
      values.sort! {|a,b| a[8].to_i <=> b[8].to_i}
      yield [ values[0][1..7], values[0][8..10], 'Y', values[1][8..10], 'Y' ]
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  SamToSingleLines::Mapper,
  SamToSingleLines::Reducer
  ).run # Execute the script