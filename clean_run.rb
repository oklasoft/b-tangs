#!/usr/bin/env ruby

require 'yaml'

def cmd_or_exit(cmd)
  puts cmd
  system cmd
  exit -1 unless 0 == $?  
end

lanes = {
6 => 'lgs000373',
7 => 'lgs101291',
5 => 'lgs103702',
3 => 'lgs103873',
4 => 'lgs303605'
}

lanes.each do |lane,sample|
  puts "#{sample} in #{lane}"
  # mkdir input dir
  cmd_or_exit("hadoop fs -mkdir '#{sample}_input/'")

  # import
  cmd_or_exit("hadoop fs -put input/#{sample}_*.txt '#{sample}_input/'")

  # clean
  cmd_or_exit("./b-tangs/run_b-tang.sh #{sample}")
end

# puts lanes.to_yaml

#lzop -dc s_1_1_*_qseq.txt.lzo > ~/tmp/b-tangs/lgs101446_1_qseq.txt
