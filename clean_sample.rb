#!/usr/bin/env ruby1.9
#
# clean_sample.rb
#
# Created by Stuart Glenn on 2010-08-08
# Copyright (c) 2010 Stuart Glenn, Oklahoma Medical Research Foundation. All rights reserved.
#
# =Description
# A quick wrapper script for all the steps to run a NGS illumina sample through
# the 'b-tangs' cleaning process to remove possible PCR artifacts
#

require 'rubygems'
require 'optparse'
require 'ostruct'

require 'omrf/fstream_logger'
require 'omrf/time_decorator'
require 'omrf/logged_external_command'

my_log = OMRF::FstreamLogger.new(STDOUT,STDERR)
my_log.extend OMRF::TimeDecorator

c = OMRF::LoggedExternalCommand.new('ls',my_log)
puts "Proc failed!" unless c.run

c2 = OMRF::LoggedExternalCommand.new('ls testies',my_log)
puts "Proc failed!" unless c2.run