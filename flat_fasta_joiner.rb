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
      return if options[:both_must_pass] && (values[0][6] != 'PASS' || values[1][6] != 'PASS')
      return if (!options[:allow_both_fail]) && (values[0][6] != 'PASS' && values[1][6] != 'PASS')

      values.sort! {|a,b| a[5].to_i <=> b[5].to_i}

      if options[:single_line]
        values.each {|v| v.shift}
        yield [ values ]
      else
        yield [ values[0] ]
        yield [ values[1] ] unless options[:first_only]
      end
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  FlatFastaJoiner::Mapper,
  FlatFastaJoiner::Reducer
  ).run # Execute the script