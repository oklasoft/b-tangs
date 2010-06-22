#!/usr/bin/env ruby

require 'yaml'

def cmd_or_exit(cmd)
  puts cmd
  system cmd
  exit -1 unless 0 == $?  
end

lanes = YAML.load_file(ARGV.first)


lanes.each do |lane,sample|
  puts "#{sample} in #{lane}"
  # mkdir input dir
  cmd_or_exit("hadoop fs -mkdir '#{sample}_input/'")

  # import
  cmd_or_exit("hadoop fs -put input/s_#{lane}_*_sorted.txt '#{sample}_input/'")

  # clean
  cmd_or_exit("ruby1.9 /home/glenns/tmp/b-tangs/b-tangs/gerald_sorted_joiner.rb --reduce_tasks=12 --run=hadoop #{sample}_input/ #{sample}_output")

  # get
  cmd_or_exit("hadoop fs -get #{sample}_output .")
  
  Dir.chdir("#{sample}_output") do
    threads = []
    {1 => '-11', 2 => '-7,12-15'}.each do |read,cuts|
      threads << Thread.new do
        cmd_or_exit("cut -f #{cuts} part-* > #{sample}_s_#{lane}_#{read}_gerald_sorted.qseq")
      end
    end
    threads.each {|t| t.join}
    # cmd_or_exit("cut -f -11 part-* > #{sample}_s_#{lane}_1_gerald_sorted.qseq")
    # cmd_or_exit("cut -f -7,12-15 part-* > #{sample}_s_#{lane}_2_gerald_sorted.qseq")
  end
end

# puts lanes.to_yaml

#lzop -dc s_1_1_*_qseq.txt.lzo > ~/tmp/b-tangs/lgs101446_1_qseq.txt