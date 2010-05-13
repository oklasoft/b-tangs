#!/usr/bin/env ruby

require 'yaml'

lanes = {
8 => 'bob1'
}

lanes.each do |lane,sample|
  puts "#{sample} in #{lane}"
  [1,2].each do |pair|
    cmd = "lzop -dc s_#{lane}_#{pair}_*_qseq.txt.lzo | awk -F '\\t' '{print \"@\"$1\"_\"$2\"_\"$3\"_\"$4\"_\"$5\"_\"$6\"_\"$7\"\\n\"$9\"\\n+\"$1\"_\"$2\"_\"$3\"_\"$4\"_\"$5\"_\"$6\"_\"$7\"\\n\"$10}' > ~/tmp/b-tangs/#{sample}_#{pair}.fastq"
    puts cmd
    system cmd
    exit -1 unless 0 == $?
  end
end

# puts lanes.to_yaml

#lzop -dc s_1_1_*_qseq.txt.lzo > ~/tmp/b-tangs/lgs101446_1_qseq.txt
