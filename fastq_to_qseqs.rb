#!/usr/bin/ruby1.9

MAX_CYCLES = 120
RJUST = MAX_CYCLES.to_s.length+1
SUFFIX = "_qseq.txt"

class Qseq
  attr_accessor :machine
  attr_accessor :run
  attr_accessor :flowcell_lane
  attr_accessor :tile
  attr_accessor :x
  attr_accessor :y
  attr_accessor :index
  attr_accessor :read
  attr_accessor :sequence
  attr_accessor :quality
  attr_accessor :filter
  
  def self.qseq_for_fastq_lines(lines)
    return nil unless 4 == lines.size
    
    qseq = self.new()
    name_line = lines[0]
    qseq.assign_attributes_from_fastq_name(name_line)
    qseq.sequence = lines[1]
    qseq.quality = lines[3]
    qseq
  end
  
  #
  # Parse a '@HWUSI-EAS1634_0042:3:1:1026:15127#0/1' styile name string into
  # the various separate attributes
  #
  def assign_attributes_from_fastq_name(name)
    parts = name.split(/:/)
    (@machine,@run) = parts.shift.split(/_/)
    @machine.sub!(/^@/,'')
    @flowcell_lane = parts.shift.to_i
    @tile = parts.shift.to_i
    @x = parts.shift.to_i
    (@y,parts) = parts.shift.split(/#/)
    @y = @y.to_i
    (@index,@read) = parts.split(/\//)
    @filter = 1
  end
  
  def to_s
    "#{@machine}\t#{@run}\t#{@flowcell_lane}\t#{@tile}\t#{@x}\t#{@y}\t#{@index}\t#{@read}\t#{@sequence}\t#{@quality}\t#{@filter}"
  end
end

def main(args)
  report_error_and_close(help(),1) unless args.size == 2
  
  prefix = args.shift
  input_fastq = args.shift
  
  if (overwrite = output_will_overwrite_something(prefix)) then
    report_error_and_close("We will overwrite, '#{overwrite}', with prefix, pick again",1)
  end
  
  translate_fastq_to_qseq_with_prefix(input_fastq,prefix)
end

# quick test to see if there are potential files that will get toasted
def output_will_overwrite_something(prefix)
  (0..MAX_CYCLES).each do |cycle|
    file = filename_for_cycle(prefix,cycle)
    return file if File.exists?(file)
  end
  return nil
end

def filename_for_cycle(prefix,cycle)
  "#{prefix}_#{cycle.to_s.rjust(RJUST,"0")}#{SUFFIX}"
end

def report_error_and_close(msg,exit_status=0,output=$stderr)
  output.puts msg
  exit exit_status
end

def help
  <<EOF
#{$0} <prefix> <fastq_file>
EOF
end

#
# open the input, looping throug making prefix_#{####_qseq.txt} files
#
def translate_fastq_to_qseq_with_prefix(input,prefix)
  fastq = []
  IO.foreach(input) do |line|
    fastq << line.chomp
    if 4 == fastq.size
      qseq = line_to_qseq(fastq)
      File.open(filename_for_cycle(qseq.tile),"a") {|f| f.puts(qseq)}
      fastq = []
    end
  end
end

#
# Take a set of 4 fastq lines and make into a qseq object
#
def line_to_qseq(lines)
  # puts "Turning #{lines[0]} into qseq"
  q = Qseq.qseq_for_fastq_lines(lines)
  # puts "\t#{q}"
  q
end


main(ARGV)