lines = {}

IO.foreach(ARGV.shift) do |line|
  lines[line.chomp] = 1
end

IO.foreach(ARGV.shift) do |line|
  next unless lines[line.split(/\t/)[3..5]]
  puts line
end