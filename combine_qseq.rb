#!/usr/bin/env ruby

require 'yaml'

lanes = {
1 => 'wanping_case',
2 => 'wanping_control',
3 => 'lgs103873',
4 => 'lgs303605',
5 => 'lgs103702',
6 => 'lgs000373',
7 => 'lgs101291'
#8 => 'Bob1'
}

lanes.each do |lane,sample|
  puts "#{sample} in #{lane}"
  [1,2].each do |pair|
    cmd = "lzop -dc s_#{lane}_#{pair}_*_qseq.txt.lzo > ~/tmp/b-tangs/#{sample}_#{pair}_qseq.txt"
    system cmd
    exit -1 unless 0 == $?
  end
end

# puts lanes.to_yaml

#lzop -dc s_1_1_*_qseq.txt.lzo > ~/tmp/b-tangs/lgs101446_1_qseq.txt
