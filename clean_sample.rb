#!/usr/bin/env ruby1.9
#
# clean_sample.rb
# Created by Stuart Glenn on 2010-08-08
#
# == Synopsis
# A quick wrapper script for all the steps to run a NGS illumina sample through
# the 'b-tangs' cleaning process to remove possible PCR artifacts
#
# == Examples
# To clean a qseq sample and save it as /Volumes/hts_raw/to_ipmort/1_cleaned_A_3/1_cleaned_A_3_{1,2}.qseq 
#  clean_sample.rb --run A --lane 3 --sample 1 /data/s_3_{1,2}.qseq /Volumes/hts_raw/to_ipmort/
#
# == Usage
#  clean_sample.rb -r RUN_NAME -l LANE_NAME -s SAMPLE_ID -b BASE_OUTPUT INPUT_SEQUENCE 
#
#  For help use clean_sample.rb -h
#
# == Options
#  -h, --help             Display this help message
#  -v, --version          Display the version information
#  -V, --verbose          Increased verbosity of output
#  -r, --run NAME         Specify the name of run from which the sequence comes
#  -l, --lane LANE        Specify the lane number/name from which the sequence comes
#  -s, --sample ID        Specify the same name or id for the sequence
#  -b, --base DIR         Specify the base folder for the output
#  -L, --log FILE         Log stats to named file
#
# ==Author
#  Stuart Glenn <Stuart-Glenn@omrf.org>
#
# ==Copyright
#  Copyright (c) 2010 Stuart Glenn, Oklahoma Medical Research Foundation. (OMRF)
#  All rights reserved.
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#  1. Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following disclaimer.
#  2. Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#  3. All advertising materials mentioning features or use of this software
#     must display the following acknowledgement:
#     This product includes software developed by the OMRF
#  4. Neither the name of the Oklahoma Medical Research Foundation nor the
#     names of its contributors may be used to endorse or promote products
#     derived from this software without specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ''AS IS'' AND ANY
#  EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#  DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

require 'rubygems'
require 'optparse'
require 'ostruct'
require 'tmpdir'

require 'omrf/fstream_logger'
require 'omrf/time_decorator'
require 'omrf/logged_external_command'
require 'omrf/dir_extensions'

class SampleCleanerApp
  
  NUMBER_FASTQ_FIELDS = 1
  NUMBER_QSEQ_FIELDS = 11
  
  HADOOP_INPUT_FOLDER = "00_input"
  HADOOP_JOINED_FOLDER = "01_joined"
  HADOOP_CLEANED_FOLDER = "02_cleaned"
  HADOOP_FINAL_FOLDER = "03_final"
  
  REJECT_FILE = 'rejects.txt'
  CONFLICTS_FILE = 'conflicts.txt'
  DIDNT_YIELD_FILE = 'didnt_yields.txt'
  
  VERSION       = "1.0.0"
  REVISION_DATE = "2010-08-06"
  AUTHOR        = "Stuart Glenn <Stuart-Glenn@omrf.org>"
  COPYRIGHT     = "Copyright (c) 2010 Oklahoma Medical Research Foundation"
  
  def initialize(args,ios = {})
    @args = args
    set_inputs_outputs(ios)
    set_default_options()
    @metrics = {:raw => 0, :passed_cleaned => 0, :rejected => 0, :conflicted => 0, :unknown => 0}
  end
  
  def run
    if options_parsed? && options_valid?
      setup_logger()
      
      return_dir = Dir.pwd
      Dir.mktmpdir do |tmp_dir|
        Dir.chdir(tmp_dir)
        clean_sample()
        Dir.chdir(return_dir)
      end
      @logger.teardown
      
    else
      @stderr.puts("")
      output_usage(@stderr)
      exit(1)
    end
  end
  
  private

  def setup_logger()
    file = File.open(File.join(final_output_dir_path(),"log.txt"),"w")
    file.sync = true
    @logger = OMRF::FstreamLogger.new(file,file)
    @logger.extend OMRF::TimeDecorator
  end
  
  def clean_sample()
    output_options(@stdout) if @options.verbose
    output_user("Working in #{Dir.getwd}", true)

    start_time = Time.now
    output_user "Starting clean of #{@options.sample} at #{start_time.utc}"
    
    try("Error getting sequence") {get_raw_input_sequence()}

    try("Error detecting sequence type") {detect_sequence_type()}
    
    output_user("Working with #{@options.sequence_format} files")

    try("Error flattening fastq") {flatten_fastq()} if :fastq == @options.sequence_format

    try("Error counting raw") {count_input_sequence()}

    output_user("Will clean #{@metrics[:raw]} reads")

    try("Error creating hadoop working dir") {make_hadoop_workdir()}
    
    try("Error putting input to hadoop") {put_input_sequnce_into_hadoop()}
    
    try("Error joining reads") {join_reads_in_hadoop()}
    
    try("Error cleaning joined reads") {clean_reads_in_hadoop_with_btangs()}
    
    try("Error finalizing cleaning joined reads") {finalize_clean_reads_in_hadoop()}
    
    try("Error output from hadoop") {get_output_out_of_hadoop()}
    
    try("Error cleaning hadoop") {clean_hadoop()}
    
    try("Error splitting output") {split_output()}
    
    try("Error counting output sequence") {count_output_sequences()}
    
    save_final_stats()
    
    end_time = Time.now
    output_user "Finished cleaning (probably) successfully at #{end_time.utc} (#{end_time-start_time})"
  end
  
  def save_final_stats
    keys = [:raw, :passed_cleaned, :rejected, :conflicted, :unknown]
    File.open(File.join(final_output_dir_path(),'stats.txt'), 'w') do |f| 
      keys.each do |k|
        f.print "\t#{k}"
      end
      f.puts
      f.print "#{cleaned_sequence_base_file_name("{1,2}")}"
      keys.each do |k|
        f.print "\t#{@metrics[k]}"
      end
      f.puts
    end
  end
  
  # take the original input files, decompressing & copying them over to our
  # temp work dir. Also replace any possible crappy CRLF action
  def get_raw_input_sequence
    @options.input_files.each_with_index do |infile,index|
      decompressor = case File.extname(infile)
        when /\.gz/
          "gzcat"
        when /\.lzo/
          "lzop -dc"
        else
          "cat"
      end
      outfile = "#{index+1}.txt"
      cmd = "#{decompressor} \"#{infile}\" | tr -d '\\r' > #{outfile}"

      wrap_command(cmd) do
        output_user("Getting raw sequence in #{infile}")        
      end
      @options.input_files[index] = File.join(Dir.pwd,outfile)
    end
    return true
  end
  
  def detect_sequence_type()
    output_user("Detecting input format via '#{@options.input_files.first}'")
    IO.foreach(@options.input_files.first) do |line|
      parts = line.chomp.split(/\t/)
      if NUMBER_FASTQ_FIELDS == parts.size
        @options.sequence_format = :fastq
      elsif NUMBER_QSEQ_FIELDS == parts.size
        @options.sequence_format = :qseq
      else
        return "unknown format with #{parts.size} fields in '#{line.chomp}' from #{@options.input_files.first}"
      end
      break
    end
  end
  
  def flatten_fastq()
    @options.input_files.each_with_index do |infile,index|
      outfile = "#{index+1}_flattened.txt"
      cmd = %{awk '{printf( "%s%s", $0, (NR%4 ? "\\t" : "\\t#{index+1}\\n") ) }' #{infile} > #{outfile}}

      c = OMRF::LoggedExternalCommand.new(cmd,@logger)
      output_user("Flattening fastq of #{infile}")
      output_user("Executing: `#{cmd}`",true)
      unless c.run
        return "#{cmd} failed: #{c.exit_status}"
      end

      File.delete(infile)
      @options.input_files[index] = File.join(Dir.pwd,outfile)      
    end
    return true
    return ""
  end
  
  def count_input_sequence()
    lines = []
    @options.input_files.each do |input_file|
      lines << count_lines(input_file)
    end
    if lines[0] != lines[1]
      return "different number of lines in '#{@options.input_files.join(",")}', #{lines.join(":")}"
    elsif 0 == lines[0] 
      return "No lines in input files '#{@options.input_files.join(",")}"
    end
    @metrics[:raw] = lines[0]
    return true
  end
  
  def count_lines(input_file)
    num_lines = 0
    IO.foreach(input_file) do
      num_lines += 1
    end
    return num_lines
  end
  
  def make_hadoop_workdir()
    cmd = "hadoop fs -mkdir #{hadoop_input_dir()}"
    wrap_command(cmd) do
      output_user("Making base hadoop dir")
    end
  end
  
  def wrap_command(cmd,&block)
    c = OMRF::LoggedExternalCommand.new(cmd,@logger)
    yield
    output_user("Executing: `#{cmd}`",true)
    unless c.run
      return "#{cmd} failed: #{c.exit_status}"
    end
    return true
  end
  
  def put_input_sequnce_into_hadoop()
    files = @options.input_files.join(" ")
    cmd = "hadoop fs -put #{files} #{hadoop_input_dir()}"
    wrap_command(cmd) do
      output_user("Putting #{files} into hadoop")
    end
  end
  
  def join_reads_in_hadoop()
    cmd = if :qseq == @options.sequence_format 
      "qseq_joiner.rb"
    elsif :fastq == @options.sequence_format
      "flat_fasta_joiner.rb"
    end
    cmd += " --run=hadoop --reduce_tasks=#{@options.num_reducers} --single_line --allow_both_fail #{hadoop_input_dir} #{hadoop_joined_dir}"
    wrap_command(cmd) do
      output_user("Joining the reads to build the 'joined_reads'")
    end
  end
  
  def clean_reads_in_hadoop_with_btangs()
    cmd = "b-tangs.rb --run=hadoop --reduce_tasks=#{@options.num_reducers} --range_start=0 --range_size=10 --similarity=1.0 --key_type=sep_joined_pairs --both_ends --include_rejects --input_format="
    cmd += if :qseq == @options.sequence_format
      "joined_qseq"
    elsif :fastq == @options.sequence_format
      "joined_fastq"
    end
    cmd += " #{hadoop_joined_dir} #{hadoop_cleaned_dir}"
    wrap_command(cmd) do
      output_user("Cleaning the joined_reads with b-tangs")
    end
  end
  
  def finalize_clean_reads_in_hadoop()
    cmd = if :qseq == @options.sequence_format 
      "joined_qseq_finisher.rb"
    elsif :fastq == @options.sequence_format
      "joined_fastq_finisher.rb"
    end
    cmd += " --run=hadoop --reduce_tasks=#{@options.num_reducers} #{hadoop_cleaned_dir} #{hadoop_final_dir}"
    wrap_command(cmd) do
      output_user("Finalizing the cleaned reads in hadoop")
    end
  end
  
  def get_output_out_of_hadoop()
    cmd = "hadoop fs -get #{hadoop_final_dir}part-\\* ."
    wrap_command(cmd) do
      output_user("Getting dataset back out of hadoop")
    end
  end
  
  def split_output()
    split_rejects() &&
    split_conflicts() &&
    split_didnt_yields() &&
    split_passings
  end
  
  def split_key_from_data_to_dest(key,dest)
    cmd = "fgrep -h #{key} part-* > #{dest}"
    wrap_command(cmd) do
      output_user("Splitting #{key} to #{dest}")
    end
  end
  
  def split_rejects()
    split_key_from_data_to_dest("REJECT",File.join(final_output_dir_path(),REJECT_FILE))
  end
  
  def split_conflicts()
    split_key_from_data_to_dest("CONFLICTS",File.join(final_output_dir_path(),CONFLICTS_FILE))
  end
  
  def split_didnt_yields()
    split_key_from_data_to_dest("DIDNT_YIELD",File.join(final_output_dir_path(),DIDNT_YIELD_FILE))
  end
  
  def split_passings
    pair_1_file = cleaned_sequence_base_file_name(1)
    pair_2_file = cleaned_sequence_base_file_name(2)
    cuts = if :qseq == @options.sequence_format
      [
        "cut -f -11 > #{File.join(final_output_dir_path(),pair_1_file)}",
        "cut -f -7,19-22 > #{File.join(final_output_dir_path(),pair_2_file)}"  
      ]
    elsif :fastq == @options.sequence_format
      [
        %{awk -F '\\t' '{print $1"\\n"$2"\\n"$3"\\n"$4}' > #{File.join(final_output_dir_path(),pair_1_file)}},
        %{awk -F '\\t' '{print $6"\\n"$7"\\n"$8"\\n"$9}' > #{File.join(final_output_dir_path(),pair_2_file)}}
      ]
    end
    cmd = "fgrep -h PASS part-* | tee >( #{cuts[0]} ) >( #{cuts[1]} ) >/dev/null"
  
    split_passing = File.join(Dir.pwd,"split_passing.sh")
    File.open(split_passing,"w") do |f|
      f.puts "/bin/bash"
      f.puts cmd
    end
    wrap_command("bash #{split_passing}") do
      output_user("Splitting the passing read to the two pair files #{pair_1_file} & #{pair_2_file}")
    end
    
  end
  
  def count_output_sequences()
    output_user("Counting up the number of reads in the various files")

    @metrics[:conflicted] = count_lines(File.join(final_output_dir_path(),CONFLICTS_FILE))
    output_user("There were #{@metrics[:conflicted]} conflicts")
    
    @metrics[:unknown] = count_lines(File.join(final_output_dir_path(),DIDNT_YIELD_FILE))
    output_user("There were #{@metrics[:unknown]} unknowns")

    @metrics[:rejected] = count_lines(File.join(final_output_dir_path(),REJECT_FILE))
    output_user("There were #{@metrics[:rejected]} rejects")
    
    cleaned_a = count_lines(File.join(final_output_dir_path(),cleaned_sequence_base_file_name(1)))
    cleaned_b = count_lines(File.join(final_output_dir_path(),cleaned_sequence_base_file_name(2)))
    
    if :fastq == @options.sequence_format
      cleaned_a /= 4
      cleaned_b /= 4
    end
    
    if cleaned_a != cleaned_b
      return "There were different number of cleaned in the two pairs #{cleaned_a} vs #{cleaned_b}"
    end
    
    output_user("There were #{cleaned_a} passing")
    @metrics[:passed_cleaned] = cleaned_a

    return true
  end
  
  def clean_hadoop()
    cmd = "hadoop fs -rmr #{base_hadoop_path()}"
    wrap_command(cmd) do
      output_user("Removing hadoop dir")
    end
  end
  
  def try(msg,&block)
    result = yield
    if result.is_a?(String) || result.is_a?(FalseClass) then
      fail "#{msg}: #{result}"
    else
      return true
    end
  end
  
  def fail(msg)
    @stderr.puts msg
    @stderr.flush
    @logger.log(:stderr,msg)
    @logger.teardown
    exit 1
  end
  
  def output_user(msg,verbosity=false)
    @stdout.puts msg if !verbosity || @options.verbose
    @logger.log(:stdout,msg)
  end
  
  def cleaned_sequence_base_file_name(pair_number)
    extension = @options.sequence_format
    "#{@options.sample}_cleaned_#{@options.run_name}_#{@options.lane}_#{pair_number}.#{extension}"
  end
  
  def final_output_dir_path
    @options.final_output_dir_path ||= File.join(@options.base_output_dir,"#{@options.sample}_#{@options.run_name}_#{@options.lane}")
  end
  
  def base_hadoop_path
    @base_hadoop_path ||= File.join(@options.sample,@options.run_name,@options.lane,"b-tangs_#{$$.to_s}")
  end
  
  def hadoop_input_dir
    File.join(base_hadoop_path(),HADOOP_INPUT_FOLDER) + "/"
  end
  
  def hadoop_joined_dir
    File.join(base_hadoop_path(),HADOOP_JOINED_FOLDER) + "/"
  end
  
  def hadoop_cleaned_dir
    File.join(base_hadoop_path(),HADOOP_CLEANED_FOLDER) + "/"
  end
  
  def hadoop_final_dir
    File.join(base_hadoop_path(),HADOOP_FINAL_FOLDER) + "/"
  end
  
  def output_help(out)
    output_version(out)
    out.puts ""
    output_usage(out)
  end
  
  def output_version(out)
    out.puts "#{File.basename(__FILE__)} Version: #{VERSION} Released: #{REVISION_DATE}"
  end
  
  def output_options(out)
    out.puts "Options:\n"
    
    @options.marshal_dump.each do |name, val|        
      out.puts "  #{name} = #{val}"
    end
  end
  
  def output_usage(out)
    out.puts <<-EOF
clean_sample.rb -r RUN_NAME -l LANE_NAME -s SAMPLE_ID -b BASE_OUTPUT INPUT_SEQUENCE

Options:
 -h, --help             Display this help message
 -v, --version          Display the version information
 -V, --verbose          Increased verbosity of output
 -r, --run NAME         Specify the name of run from which the sequence comes
 -l, --lane LANE        Specify the lane number/name from which the sequence comes
 -s, --sample ID        Specify the same name or id for the sequence
 -b, --base DIR         Specify the base folder for the output
 -L, --log FILE         Log stats to named file
    
    EOF
  end
  
  def set_default_options()
    @options = OpenStruct.new(
      :run_name => nil,
      :lane => nil,
      :sample => nil,
      :input_files  => nil,
      :base_output_dir => nil,
      :verbose => false,
      :log_file => nil,
      :num_reducers => 30
    )
  end
  
  def options_valid?
    sequence_input_valid? &&
    base_directory_valid? &&
    run_name_valid? &&
    lane_name_valid? &&
    sample_id_valid? &&
    final_output_dir_valid?
  end
  
  def final_output_dir_valid?
    if File.exists?(final_output_dir_path())
      if !File.directory?(final_output_dir_path())
        @stderr.puts("Final output location, #{final_output_dir_path()}, exists and is not a directory")
        return false
      elsif !File.writable?(final_output_dir_path())
        @stderr.puts("Final output location, #{final_output_dir_path()}, is not writable")
        return false
      elsif !Dir.empty?(final_output_dir_path())
        @stderr.puts("Final output location, #{final_output_dir_path}, should be empty")
        return false
      end
      return true
    end
    Dir.mkdir(final_output_dir_path())
    return true
  end
  
  def run_name_valid?
    valid_string_name?(@options.run_name,"run name")
  end
  
  def lane_name_valid?
    valid_string_name?(@options.lane,"lane name")
  end
  
  def sample_id_valid?
    valid_string_name?(@options.sample,"sample name")
  end
  
  def valid_string_name?(key,mesg_name)
    if nil == key
      @stderr.puts("Missing #{mesg_name} option")
      return false
    end
    key.strip!
    key.downcase!
    key.gsub!(/ /,'_')
    
    if key.empty? 
      @sdterr.puts("Missing a full string for the #{mesg_name}")
      return false
    end
    unless key =~ /^[a-zA-Z0-9_-]+$/
      @stderr.puts("#{mesg_name} can only be a simple alpha numeric string")
      return false
    end
    return true
    
  end
  
  def base_directory_valid?
    if nil == @options.base_output_dir
      @stderr.puts("Missing output base directory option")
      return false
    end
    
    return valid_dir?(@options.base_output_dir)
  end
  
  def valid_dir?(dir)
    unless File.directory?(dir)
      @stderr.puts("Output base is not a directory")
      return false
    end
    
    unless File.writable?(dir)
      @stderr.puts("Unable to write to output directory")
      return false
    end

    return true
  end
  
  def sequence_input_valid?
    if nil == @options.input_files || 2 != @options.input_files.size
      @stderr.puts "Two sequence files are required"
      return false
    end
    @options.input_files.each do |f|
      unless File.readable?(f)
        @stderr.puts "Unable to read input sequence file: #{f}"
        return false
      end
    end
    @options.input_files.sort!
    return true
  end
  
  def options_parsed?
    opts = OptionParser.new() do |opts|
      opts.on('-v','--version') { output_version($stdout); exit(0) }
      opts.on('-h','--help') { output_help($stdout); exit(0) }
      opts.on('-V', '--verbose')    { @options.verbose = true }
      
      opts.on("-r","--run", "=REQUIRED") do |run_name|
        @options.run_name = run_name
      end
      
      opts.on("-l","--lane", "=REQUIRED") do |lane_name|
        @options.lane = lane_name
      end
      
      opts.on("-s","--sample", "=REQUIRED") do |sample_id|
        @options.sample = sample_id
      end
      
      opts.on("-L","--log", "=REQUIRED") do |log_destination|
        @options.log_file = log_destination
      end

      opts.on("-b","--base", "=REQUIRED") do |output_destination|
        @options.base_output_dir = output_destination
      end
    end
    
    opts.parse!(@args) rescue return false
    @options.input_files = @args
    return true
  end
  
  def set_inputs_outputs(ios)
    @stdin = ios[:stdin] || STDIN
    @stdout = ios[:stdout] || STDOUT
    @stderr = ios[:stderr] || STDERR
  end
end

if $0 == __FILE__
  SampleCleanerApp.new(ARGV.clone).run
end
