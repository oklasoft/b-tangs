#  Created by Stuart B Glenn on 2008-04-11.
#  Copyright (c) 2008 Oklahoma Medical Research Foundation. All rights reserved.

# A wrapper around popen4 and threads to run external commands and log their
# STDOUT & STDERR in some manner
module OMRF
class LoggedExternalCommand
  require 'popen4'
  
  attr_reader :command
  attr_reader :exit_status
  
  def initialize(command,output)
    @command = command
    @output = output
  end
  
  def run()
    status = POpen4::popen4(@command) do |stdout, stderr, stdin, pid|
      stdin.close
      
      out = Thread.new do
        non_blocking_read_to_log(stdout,:stdout)
      end
      
      err = Thread.new do
        non_blocking_read_to_log(stderr,:stderr)
      end
      
      [out,err].each { |t| t.join}
    end
    if nil == status
      @exit_status = 255
      @output.log(:stderr,"Unable to start command")
    else
      @exit_status = status.exitstatus
    end
    return 0 == @exit_status
  end
  
  def non_blocking_read_to_log(input,type)
    while result = IO.select([input], nil, nil, 10)
      next if result.empty?
      begin
        line = input.readline()
      rescue EOFError
        break
      end
      @output.log(type,line)
      break if input.closed?
    end        
  end
end
end