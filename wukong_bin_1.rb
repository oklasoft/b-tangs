require 'wukong'
require 'amatch'

Wukong.logger = Wukong.default_log4r_logger
Wukong.logger.outputters = Log4r::FileOutputter.new("f1", :filename => "./bin1.log")

module SequenceBinner
  
  class Mapper < Wukong::Streamer::LineStreamer
    
    def process line
      (read_name,sequence,quality_name,quality) = line.split(/\t/)
      yield [sequence[0,20], read_name, sequence, quality]
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
      best_name = best[NAME_COL]
      yield [ best_sequence[20,20], best[NAME_COL], best_sequence, best[QUALITY_COL] ]
      levenshtein_pattern = Amatch::Levenshtein.new(best_sequence)
      values.each do |v|
        if best_sequence == v[SEQUENCE_COL] || levenshtein_pattern.similar(v[SEQUENCE_COL]) >= 0.95 then
          Wukong.logger.info "#{v[NAME_COL]} matches #{best_name}"
          next
        end
        yield [ v[SEQUENCE_COL][20,20], v[NAME_COL], v[SEQUENCE_COL], v[QUALITY_COL] ]
      end #values
      
    end #finalize
    
  end #reducer
end
    
Wukong::Script.new(
  SequenceBinner::Mapper,
  SequenceBinner::Reducer
  ).run # Execute the script