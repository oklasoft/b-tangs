#!/usr/bin/env ruby1.9

class Logger
  def initialize(*opts)
    
  end
  
  def log(type,msg)
    raise "Must implement!"
  end
end

class FstreamLogger < Logger
  def initialize(out,err)
    super
    @out = out
    @err = err
  end
  
  def log(type,msg)
    case type
      when :stderr
        @err.puts "ERROR: #{msg}"
      when :stdout
        @out.puts "OUTPUT: #{msg}"
    end
  end
end

module TimeDecorator
  def log(type,msg)
    super(type,"#{Time.now.utc}: #{msg}")
  end
end

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
    @exitstatus = status.exitstatus
    return 0 == @exitstatus
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

my_log = FstreamLogger.new(STDOUT,STDERR)
my_log.extend TimeDecorator

c = LoggedExternalCommand.new('ls',my_log)
puts "Proc failed!" unless c.run

c2 = LoggedExternalCommand.new('ls testies',my_log)
puts "Proc failed!" unless c2.run