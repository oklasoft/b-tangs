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
  VERSION       = "1.0.0"
  REVISION_DATE = "2010-08-06"
  AUTHOR        = "Stuart Glenn <Stuart-Glenn@omrf.org>"
  COPYRIGHT     = "Copyright (c) 2010 Oklahoma Medical Research Foundation"
  
  def initialize(args,ios = {})
    @args = args
    set_inputs_outputs(ios)
    set_default_options()
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
    else
      @stderr.puts("")
      output_usage(@stderr)
      exit(1)
    end
  end
  
  private

  def setup_logger()
    file = File.open(File.join(final_output_dir_path(),"log.txt"),"w")
    @logger = OMRF::FstreamLogger.new(file,file)
    @logger.extend OMRF::TimeDecorator
  end
  
  def clean_sample()
    output_options(@stdout) if @options.verbose
    output_user("Working in #{Dir.getwd}", true)

    start_time = Time.now
    output_user "Starting clean of #{@options.sample} at #{start_time.utc}"
    
    try("Error getting sequence") {get_raw_input_sequence()}
    
    end_time = Time.now
    output_user "Finished cleaning (probably) successfully at #{end_time.utc} (#{end_time-start_time})"
  end
  
  def get_raw_input_sequence
    return "ah fuck"
  end
  
  def try(msg,&block)
    result = yield
    if result.is_a?(String) then
      fail "#{msg}: #{result}"
    else
      return true
    end
  end
  
  def fail(msg)
    @stderr.puts msg
    @logger.log(:stderr,msg)
    @logger.teardown
    exit 1
  end
  
  def output_user(msg,verbosity=false)
    @stdout.puts msg if !verbosity || @options.verbose
    @logger.log(:stdout,msg)
  end
  
  def final_output_dir_path
    @options.final_output_dir_path ||= File.join(@options.base_output_dir,"#{@options.sample}_#{@options.run_name}_#{@options.lane}")
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
      :log_file => nil
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


# c = OMRF::LoggedExternalCommand.new('ls',my_log)
# puts "Proc failed!" unless c.run
# 
# c2 = OMRF::LoggedExternalCommand.new('ls testies',my_log)
# puts "Proc failed!" unless c2.run