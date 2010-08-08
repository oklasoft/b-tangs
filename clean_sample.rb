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
#  clean_sample.rb -r RUN_NAME -l LANE_NAME -s SAMPLE_ID INPUT_SEQUENCE BASE_OUTPUT
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

require 'omrf/fstream_logger'
require 'omrf/time_decorator'
require 'omrf/logged_external_command'

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
      output_options(@stdout) if @options.verbose
      
      # do the work
    else
      output_usage(@stderr)
      exit(1)
    end
  end
  
  private
  
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
clean_sample.rb -r RUN_NAME -l LANE_NAME -s SAMPLE_ID INPUT_SEQUENCE BASE_OUTPUT

Options:
 -h, --help             Display this help message
 -v, --version          Display the version information
 -V, --verbose          Increased verbosity of output
 -r, --run NAME         Specify the name of run from which the sequence comes
 -l, --lane LANE        Specify the lane number/name from which the sequence comes
 -s, --sample ID        Specify the same name or id for the sequence
 -L, --log FILE         Log stats to named file
    
    EOF
  end
  
  def set_default_options()
    @options = OpenStruct.new(
      :run => nil,
      :lane => nil,
      :sample => nil,
      :input_files  => nil,
      :base_output_dir => nil,
      :verbose => false,
      :log_file => nil
    )
  end
  
  def options_valid?
    true
  end
  
  def options_parsed?
    opts = OptionParser.new() do |opts|
      opts.on('-v','--version') { output_version($stdout); exit(0) }
      opts.on('-h','--help') { output_help($stdout); exit(0) }
      opts.on('-V', '--verbose')    { @options.verbose = true }
      
    end
    
    opts.parse!(@args) rescue return false
    return true
  end
  
  def set_inputs_outputs(ios)
    @stdin = ios[:stdin] || STDIN
    @stdout = ios[:stdin] || STDOUT
    @stderr = ios[:stdin] || STDERR
  end
end

if $0 == __FILE__
  SampleCleanerApp.new(ARGV.clone).run
end

# my_log = OMRF::FstreamLogger.new(STDOUT,STDERR)
# my_log.extend OMRF::TimeDecorator
# 
# c = OMRF::LoggedExternalCommand.new('ls',my_log)
# puts "Proc failed!" unless c.run
# 
# c2 = OMRF::LoggedExternalCommand.new('ls testies',my_log)
# puts "Proc failed!" unless c2.run