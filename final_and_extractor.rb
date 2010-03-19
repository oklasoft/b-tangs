lines = {}

IO.foreach(ARGV.shift) do |line|
lines[line.chomp.split(/:/).last.to_i] = 1
end

num = 0
IO.foreach(ARGV.shift) do |line|
num += 1
next unless lines[num]
puts line
end