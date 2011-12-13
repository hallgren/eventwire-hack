module Eventwire
  module Publisher
    # def publish_command(command_name, command_data = nil)
    #   Eventwire.publish_command command_name, command_data
    # end

    # def publish_event(event_name, event_data = nil)
    #   Eventwire.publish_event event_name, event_data
    # end

    # def publish_error(event_data = nil)
    #   Eventwire.publish_error event_data
    # end

    def handle_event(event_name, event_data = nil)
      Eventwire.handle_event event_name, event_data
    end


  end
end
