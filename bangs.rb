#!/usr/bin/ruby

# A quick hack of a script to profile a couple of comparison ideas

require 'rubygems'
require 'sqlite3'
require 'amatch'

DB = SQLite3::Database.new("./test.sqlite3")

@inserter_1 = nil
@inserter_2 = nil


def key_for(seq)
  seq
  #("%03d" % "0x#{seq.gsub(/T/,'B').gsub(/N/,'D').gsub(/G/,'E')}")
  #.to_i.to_s(16).gsub(/e/,'g').gsub(/d/,'n').gsub(/b/,'t').upcase
end


def prep_db()
  DB.execute("PRAGMA fullfsync = false")
  DB.execute("PRAGMA journal_mode = MEMORY")
  DB.execute("PRAGMA synchronous = off")
  DB.execute("PRAGMA temp_store = 2")
  DB.execute("PRAGMA page_size=32768")
  DB.execute("PRAGMA cache_size = 4000")
  
   DB.execute(<<-END_TABLE.strip) 
   CREATE TABLE reads_1 ( name  TEXT, 
                      sequence TEXT,
                      quality TEXT,
                      read_key TEXT,
                      primary key (name) ) 
   END_TABLE
   DB.execute(<<-END_TABLE.strip) 
   CREATE TABLE reads_2 ( name  TEXT, 
                      sequence TEXT, 
                      quality TEXT,
                      read_key TEXT,
                      comp_id INTEGER,
                      score TEXT,
                      primary key (name) ) 
   END_TABLE

  @inserter_1 = DB.prepare("INSERT INTO reads_1(name, sequence, quality, read_key) VALUES(?,?,?,?)")

end

def add_db_indexes()
  # DB.execute("CREATE INDEX reads_1_read_key_idx on reads_1(read_key);")
  DB.execute("CREATE INDEX reads_2_read_key_idx on reads_2(read_key);")  
  
  @inserter_2 = DB.prepare("INSERT INTO reads_2(name, sequence, quality, read_key,comp_id,score) VALUES(?,?,?,?,?,?)")
end

def add_read_1(id,read,quality)
  @inserter_1.execute(id,read,quality,key_for(read[0,20]))
end

def add_read_2(id,read,quality,comp,score)
  @inserter_2.execute(id,read,quality,key_for(read[20,20]),comp,score)
end

def solexa_quality(char)
  10.0*Math.log10(10.0**((char.ord-64.0)/10.0)+1.0)
end

def quality_sum(quality)
  quality.each_char.inject(0.0) {|sum,char| sum += solexa_quality(char)}
end

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
    sum = phred_quality_sum(qual)
    if  sum > max then
      max = sum
      index = i
    end
  end
  index
end


# first bin is made by grouping on identical first 20 bases
def bin_reads()
  # start = Time.now()
  File.open("bin1-output.txt","w") do |log|
    
    DB.execute("BEGIN")  
    commit_number = 0
    
    DB.execute("SELECT group_concat(name), group_concat(sequence), group_concat(quality,' ') from reads_1 group by read_key") do |row|
      names = row[0].split(/,/)
      sequences = row[1].split(/,/)
      qualities = row[2].split(/ /)
      best_index = top_quality_index(qualities)
      current_name = names.delete_at(best_index)
      current_sequence = sequences.delete_at(best_index)
      current_quality = qualities.delete_at(best_index)
      add_read_2(current_name,current_sequence,current_quality,nil,nil)
      commit_number+=1
      sequences.each_with_index do |seq,i|
        if seq == current_sequence || current_sequence.levenshtein_similar(seq) >= 0.95 then
          log.puts "#{names[i]} matches #{current_name}"
          next
        end
        add_read_2(names[i],seq,qualities[i],nil, nil)
        # add_read_2(names[i],seq,qualities[i],current_name,current_sequence.levenshtein_similar(seq))
        commit_number+=1
      end
      
      if 0==commit_number % 10000
        DB.execute("COMMIT")
        DB.execute("BEGIN")
      end
      
      
    end # select

    DB.execute("COMMIT")

  end # file open
  # puts Time.now-start
end


# now of everything that is left we bin on the next 20 bases
def report_reads()
  # DB.results_as_hash  = true
  File.open("bin2-output.txt","w") do |log|
    commit_number = 0
    
    File.open("output.txt","w") do |out|
      # start = Time.now()
      DB.execute("SELECT group_concat(name), group_concat(sequence), group_concat(quality,' ') from reads_2 group by read_key order by name") do |row|
        names = row[0].split(/,/)
        sequences = row[1].split(/,/)
        qualities = row[2].split(/ /)
        best_index = top_quality_index(qualities)
        current_name = names.delete_at(best_index)
        current_sequence = sequences.delete_at(best_index)
        current_quality = qualities.delete_at(best_index)
        out.puts "#{current_name}\n#{current_sequence}\n#{current_name.sub(/^@/,'+')}\n#{current_quality}"
        sequences.each_with_index do |seq,i|
          if seq == current_sequence || current_sequence.levenshtein_similar(seq) >= 0.95 then
            log.puts "#{names[i]} matches #{current_name}"
            next
          end
          out.puts "#{names[i]}\n#{seq}\n#{names[i].sub(/^@/,'+')}\n#{qualities[i]}"
          commit_number+=1
        end
        if 0==commit_number % 500
          out.flush
        end
      end #select
      # puts Time.now-start
    end
  end
end

def log_msg(msg)
  STDERR.puts("#{Time.now.utc.strftime("%Y-%m-%dT%H:%M:%S %Z")}: #{msg}")
end

log_msg("Starting")

prep_db()

f = File.new(ARGV.shift)
begin
 DB.execute("BEGIN")  
 while (read_name = f.gets)
   sequence = f.gets.chomp
   quality_name = f.gets.chomp
   quality = f.gets.chomp
   add_read_1(read_name.chomp,sequence,quality)
   if 0==f.lineno % 100000
     print "."
     DB.execute("COMMIT")
     DB.execute("BEGIN")
   end
 end
end
puts ""
DB.execute("COMMIT")

log_msg "Adding indexes"

add_db_indexes()

log_msg "Indexes added"

log_msg "Initial Binning"

bin_reads()

log_msg "Final reporting"

report_reads

log_msg("Finished")
