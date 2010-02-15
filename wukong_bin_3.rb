#!/usr/bin/ruby1.9
require 'wukong'
require 'amatch'

module SequenceBinner
  
  class Mapper < Wukong::Streamer::LineStreamer
    
    def process line
      (read_name,sequence,quality) = line.split(/\t/)
      yield [sequence[40,60], read_name, sequence, quality]
    end
  end

  class Reducer < Wukong::Streamer::ListReducer
    
    KEY_COL = 0
    NAME_COL = 1
    SEQUENCE_COL = 2
    QUALITY_COL = 3
    
    def phred_quality(char)
      char.ord - 33
    end
    
    def phred_quality_sum(quality)
      quality.each_char.inject(0.0) {|sum,char| sum += phred_quality(char)}
    end

    def top_quality_index(qualities)
      max = -100.0
      index = 0
      qualities.each_with_index do |qual,i|
        sum = phred_quality_sum(qual[QUALITY_COL])
        if  sum > max then
          max = sum
          index = i
        end
      end
      index
    end
    
    # values is an array of key, read_name, sequence, quality
    def finalize
      best_index = top_quality_index(values)
      best = values.delete_at(best_index)
      best_sequence = best[SEQUENCE_COL]
      # yield [ best_sequence[20,20], best[NAME_COL], best_sequence, best[QUALITY_COL] ]
      yield best[NAME_COL]
      yield best_sequence
      yield best[NAME_COL].sub(/^@/,'+')
      yield best[QUALITY_COL]
      levenshtein_pattern = Amatch::Levenshtein.new(best_sequence)
      values.each do |v|
        if best_sequence == v[SEQUENCE_COL] || levenshtein_pattern.similar(v[SEQUENCE_COL]) >= 0.90 then
          next
        end
        yield v[NAME_COL]
        yield v[SEQUENCE_COL]
        yield v[NAME_COL].sub(/^@/,'+')
        yield v[QUALITY_COL]
      end #values
      
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  SequenceBinner::Mapper,
  SequenceBinner::Reducer
  ).run # Execute the script