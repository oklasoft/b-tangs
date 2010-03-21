#!/usr/bin/ruby1.9
require 'wukong'

module QseqJoiner

  class Mapper < Wukong::Streamer::LineStreamer

    #
    # lzop -dc s_2_2_*_qseq.txt.lzo|awk -F '\t' '{print $1"_"$2"_"$3"_"$4"_"$5"_"$6"_"$7"\t"$9"\t"$10"\t"$11}' > /tmp/lgs101823_2_qseq.txt
    #
    def process line
      # key sequence 
      parts = line.chomp.split(/\t/)      
      yield [parts[0..6].join("_"), *parts]
    end

  end

  class Reducer < Wukong::Streamer::ListReducer

    # values is an array of key, qseq fields
    def finalize
      next unless 2 == values.size
      values.sort! {|a,b| a[8].to_i <=> b[8].to_i}
      yield [ v[0][1..7], v[0][8], v[1][8] ]
      end #values      
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  QseqJoiner::Mapper,
  QseqJoiner::Reducer
  ).run # Execute the script