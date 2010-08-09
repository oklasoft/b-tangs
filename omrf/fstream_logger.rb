#  Created by Stuart B Glenn on 2008-04-11.
#  Copyright (c) 2008 Oklahoma Medical Research Foundation. All rights reserved.

# A simple logger that can use file streams (files, stdin, stdout, you know)
module OMRF
  require 'omrf/simple_logger'
  class FstreamLogger < SimpleLogger
    def initialize(out,err)
      super
      @out = out
      @err = err
    end
    
    def teardown
      @out.close
      @err.close unless @err.closed?
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
end