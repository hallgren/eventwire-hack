require 'eventwire/version'
require 'eventwire/publisher'
require 'eventwire/subscriber'
require 'eventwire/drivers'

module Eventwire
  
  # def self.publish_command(command_name, command_data = nil)
  #   driver.publish_command command_name, command_data
  # end

  # def self.publish_event(event_name, event_data = nil)
  #   driver.publish_event event_name, event_data
  # end

  # def self.publish_error(event_name, event_data = nil)
  #   driver.publish_error event_name, event_data
  # end

  def self.handle_event(event_name, event_data = nil)
    driver.handle_event event_name, event_data
  end

  def self.subscribe(event_name, handler_id, &handler)
    driver.subscribe event_name, handler_id do |data|
      begin
        handler.call build_event(data) 
      rescue Exception => ex
        @error_handler.call(ex) if @error_handler
      end
    end
  end
  
  def self.build_event(data)
    data && Struct.new(*data.keys.map(&:to_sym)).new(*data.values)
  end
  
  def self.driver
    @driver ||= Drivers::AMQP_UPPTEC.new
  end
  
  def self.driver=(driver)
    klass = Drivers.const_get(driver.to_sym) if driver.respond_to?(:to_sym)
    @driver = klass ? klass.new : driver
  end
  
  def self.start_worker
    driver.start
  end

  def self.start_worker_send
    driver.send_worker
  end
  
  def self.stop_worker
    driver.stop
  end
  
  def self.on_error(&block)
    @error_handler = block
  end
  
end
