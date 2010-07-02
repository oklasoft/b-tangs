#!/usr/bin/ruby1.9
require 'wukong'

module FlatFastaJoiner

  class Mapper < Wukong::Streamer::LineStreamer

    #
    # name read name quality number
    #
    def process line
      parts = line.chomp.split(/\t/)
      # the name has the /number, we need to remove it
      key = parts[0].split(/\//).first
      yield [key, *parts]
    end

  end

  class Reducer < Wukong::Streamer::ListReducer

    # values is an array of key, fields (name read name quality number)
    def finalize
      return unless 2 == values.size
      values.sort! {|a,b| a[4].to_i <=> b[4].to_i}
      yield [ values[0] ]
      yield [ values[1] ]
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  FlatFastaJoiner::Mapper,
  FlatFastaJoiner::Reducer
  ).run # Execute the script