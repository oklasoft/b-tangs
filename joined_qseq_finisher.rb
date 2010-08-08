#!/usr/bin/ruby1.9
require 'wukong'

module JoinedQseqFinisher

  class Mapper < Wukong::Streamer::LineStreamer

    #
    # name read name quality number
    #
    def process line
      parts = line.chomp.split(/\t/)      
      yield [parts[0..6].join("_"), *parts]
    end

  end

  class Reducer < Wukong::Streamer::ListReducer

    SEQ_INDEX = 9
    QUALITY_INDEX = 10
    PAIR_NUMBER_INDEX = 8
    
    SECOND_READ_OFFSET = 11
    
    STATUS_INDEX = 23
    STATUS_DETAILS_INDEX = 24

    # values is an array of key, fields (name read name quality number name read name quality number status)
    def finalize
      return unless 2 == values.size # we need at least a pair & everything should be a pair
      
      statii = values.map { |v| v[STATUS_INDEX] }.uniq
      
      if 1 == statii.size then
        read_pair = values.first
        read_pair.shift # get rid of the key
        yield [read_pair]
        return
      end
      
      unless statii.include?("REJECT") then
        # we either PASSED in the clear or PASSED as the BEST for something or PASSED by not matching
        read_pair = values.first
        read_pair.shift # get rid of the key
        yield [ read_pair, values[1][STATUS_INDEX], values[1][STATUS_DETAILS_INDEX] ]
        return
      end
      
      if statii.include?("PASS_DIDNT_MATCH") && statii.include?("REJECT") then
        # we were didn't match the one, but rejected the other, we REJECT overall
        read_pair = values.detect {|v| "REJECT" == v[STATUS_INDEX] }
        read_pair.shift # get rid of the key
        yield [ read_pair ]
        return
      end      
      
      if statii.include?("PASS_BEST_FOR") && statii.include?("REJECT") then
        # we were the best & we were rejected!
        read_pair = values.first
        read_pair[STATUS_INDEX] = "CONFLICT_BR"
        read_pair[STATUS_DETAILS_INDEX] += values[1][STATUS_DETAILS_INDEX]

        read_pair.shift # get rid of the key
        yield [ read_pair ]
        return
      end
      
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  JoinedQseqFinisher::Mapper,
  JoinedQseqFinisher::Reducer
  ).run # Execute the script