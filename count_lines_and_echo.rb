#!/usr/bin/env ruby1.9

num_lines = 0
STDIN.each do |line|
  num_lines += 1
  print line
end
File.open(ARGV.shift,"w") do |f|
  f.puts num_lines
end
