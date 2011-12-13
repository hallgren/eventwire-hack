module Eventwire
  module Subscriber
    def self.included(base)
      base.extend DSL
    end
    
    module DSL
      def on(event_name, &handler)
        Eventwire.subscribe event_name, "#{name}::#{increment_handler_counter}", &handler
      end
      
      private
      
      def increment_handler_counter
        @_handler_counter ||= 0
        @_handler_counter +=  1
      end
    end
  end
end
