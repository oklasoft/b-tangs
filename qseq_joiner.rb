#!/usr/bin/ruby1.9
require 'wukong'

module QseqJoiner

  class Mapper < Wukong::Streamer::LineStreamer

    #
    # lzop -dc s_2_2_*_qseq.txt.lzo|awk -F '\t' '{print $1"_"$2"_"$3"_"$4"_"$5"_"$6"_"$7"\t"$9"\t"$10"\t"$11}' > /tmp/lgs101823_2_qseq.txt
    #
    def process line
      parts = line.chomp.split(/\t/)      
      yield [parts[0..6].join("_"), *parts]
    end

  end

  class Reducer < Wukong::Streamer::ListReducer

    # values is an array of key, qseq fields
    def finalize
      return unless 2 == values.size
      
      return if options[:both_must_pass] && (values[0].last != 'PASS' || values[1].last != 'PASS')
      return if (!options[:allow_both_fail]) && (values[0].last != 'PASS' && values[1].last != 'PASS')
      
      values.sort! {|a,b| a[8].to_i <=> b[8].to_i}
      if options[:single_line]
        values.each {|v| v.shift}
        yield [ values ]
      else
        yield [ values[0][1..7], values[0][8..11], values[1][8..11] ]
      end
      # yield [ values[0][8], values[0][1..7], values[0][8..11] ]
      # yield [ values[1][8], values[0][1..7], values[1][8..11] ]
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  QseqJoiner::Mapper,
  QseqJoiner::Reducer
  ).run # Execute the script