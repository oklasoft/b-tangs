#  Created by Stuart B Glenn on 2008-04-11.
#  Copyright (c) 2008 Oklahoma Medical Research Foundation. All rights reserved.

# Real simple logger, the base that things much implement
module OMRF
  class SimpleLogger
    def initialize(*opts)

    end
    
    def teardown
      
    end

    def log(type,msg)
      raise "Must implement!"
    end
  end
end