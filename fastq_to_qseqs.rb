#!/usr/bin/ruby1.9

def main(args)
  report_error_and_close help() unless args.size == 2
  
  prefix = args.shift
  input_fastq = args.shift
  
  translate_fastq_to_qseq_with_prefix(input_fastq,prefix)
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
  
end

main(ARGV)