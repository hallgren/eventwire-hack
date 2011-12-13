require 'bunny'
require 'amqp'
require 'uuid'
require "rubygems"

class Eventwire::Drivers::AMQP_UPPTEC
  
 # @@subscriptions = nil

  class EMWrapper

    at_exit {
      puts "EXIT!!!!!!!"
      sleep 1
      #EM.stop unless @reactor_running
    }

    def stop
      EM.stop unless @reactor_running
    end
    
    def initialize(&block)
      puts "init"
      @reactor_running = EM.reactor_running?
      if @reactor_running
        block.call(self)
      else
        EM.run { block.call(self) }
      end
    end

    def initialize()
      puts "init"
    end
  end
  
  

  def send_worker
    puts "Start send_worker"

    loop do

      current_message = nil

      begin

        puts "BEGIN SEND_WORKER LOOP"

        mq_list = MessageQueue.all

        mq_list.each do |mq|

          return_ok = false
          current_message = mq

          case mq.message_type
            when "event"
              return_ok = publish_event(mq.message_name, mq.message_data)
            when "command"
              return_ok = publish_command(mq.message_name, mq.message_data)
            when "error"
              return_ok = publish_error(mq.message_data)
          end
          
          if return_ok
            mq.destroy
          else
            puts "Handle retry"
            #mq.retry
          end

        end
        
      rescue
        puts "exception in sending of a message #{current_message.message_data}"
        event_data = parse_json current_message.message_data
        event_data["error_message"] = "Exception in sending message to Broker #{Rails.application.class.parent_name}"
        mq = MessageQueue.new
        mq.message_type = "error"
        mq.message_name = "" #Not used
        mq.message_data = event_data.to_json

        mq.save
        puts "error event put in queue"
      end

      sleep 10
    
    end
  end

  def subscribe(event_name, handler_id, &handler)
    subscriptions << [event_name, handler_id, handler]
  end

  def start
    puts "Start"
    EM.run do|em|
      @em = em
      AMQP.connect(:host => APP_CONFIG["eventwire_event_host"], :port =>  APP_CONFIG["eventwire_event_port"], :user =>  APP_CONFIG["eventwire_event_user"], :pass =>  APP_CONFIG["eventwire_event_password"], :vhost =>  APP_CONFIG["eventwire_event_vhost"]) do |connection|
    
        channel = AMQP::Channel.new(connection, 2, :auto_recovery => true) 
        puts "connected to #{APP_CONFIG["eventwire_event_host"]} #{APP_CONFIG["eventwire_event_port"]} #{APP_CONFIG["eventwire_event_user"]} #{APP_CONFIG["eventwire_event_password"]} #{APP_CONFIG["eventwire_event_vhost"]} Exchange: #{APP_CONFIG["eventwire_event_exchange"]} Queue: #{APP_CONFIG["eventwire_event_queue"]}"
        puts subscriptions.count

        if channel.auto_recovering?
          puts "Channel #{channel.id} IS auto-recovering"
        end

        connection.on_tcp_connection_loss do |conn, settings|
          
          puts "[network failure] Trying to reconnect..."
          conn.reconnect(false, 2)

        end

        connection.on_recovery do |conn, settings|
          puts "[recovery] Connection has recovered"

          #Send error event abount the downtime
          mq = MessageQueue.new
          mq.message_type = "error"
          mq.message_name = "" #Not used
          mq.message_data = {:error_message => "Connection to Broker has been down: #{Rails.application.class.parent_name}"}.to_json
          mq.save
          
        end

        #Setup error queue
        error_queue = channel.queue(APP_CONFIG["eventwire_error_queue"], :durable => true)
        error_queue.bind(APP_CONFIG["eventwire_event_exchange"], :routing_key => "event_error", :durable => :true)

        #Setup event handler queue
        event_exchange = channel.topic(APP_CONFIG["eventwire_event_exchange"], :durable => true)
        event_queue    = channel.queue(APP_CONFIG["eventwire_event_queue"], :durable => true, :auto_delete => false)

        #Bind to events
        subscriptions.each do |subscription|
          puts subscription[0]
          event_queue.bind(APP_CONFIG["eventwire_event_exchange"], :routing_key => subscription[0].to_s, :durable => true)
        end
        
        #Subscription 

        event_queue.subscribe(:ack => true) do |header, body|
          puts " [event] #{header.routing_key}: ---  #{body}"
          
          handler_result_ok = handle_event(header.routing_key, body)
          header.ack
          if !handler_result_ok
            publish_error body
          end

          

          puts "HANDLER RESULT #{handler_result_ok}"
          
        end
      end
    end
  end

  

 
  def stop
    puts "STOP!!!!!"
    AMQP.stop { EM.stop }
  end
  
  def purge
    Bunny.run do |mq|
      subscriptions.group_by(&:first).each do |event_name, _|
        mq.exchange(event_name, :type => :fanout).delete
      end
      subscriptions.group_by(&:second).each do |handler_id, _|
        mq.queue(handler_id).delete
      end
    end
  end
  
  def parse_json(json)
    json != 'null' && JSON.parse(json) 
  end

  def subscriptions
    @subscriptions ||= []
  end
  

  def handle_event(event_name, event_data = nil)

    begin

      data = parse_json(event_data)
      
      #Check if event is already handled
      if Event.find_all_by_event_uuid(data["event_uuid"]).count == 0
        
        #find handler in the subscriptions
        subscriptions.each do |subscription|
          if (subscription[0].to_s == event_name.to_s)
            subscription[2].call data            
          end
        end

        event = Event.new
        event.event_uuid = data["event_uuid"]
        event.data = data
        event.save

      end

    rescue
      return false 
    end

    return true

  end

private

  def publish_command(command_name, command_data = nil)

    begin
      puts "PUBLISH_COMMAND"
      b = Bunny.new(:host => APP_CONFIG["eventwire_command_host"], :port =>  APP_CONFIG["eventwire_command_port"], :user =>  APP_CONFIG["eventwire_command_user"], :pass =>  APP_CONFIG["eventwire_command_password"], :vhost =>  APP_CONFIG["eventwire_command_vhost"])
      b.start
      ex = b.exchange(APP_CONFIG["eventwire_command_exchange"], :type => :topic, :persistent => true, :durable => true)
      ex.publish(command_data.to_json, :key => command_name.to_s, :persistent => true)
      b.stop
    rescue Bunny::ServerDownError, Bunny::ConnectionError => e
      #If connection to broker is down stop all messages in the loop
      throw
    rescue 
      return false
    end
    puts "END OF publish command"
    return true

  end

  def publish_event(event_name, event_data = nil)

    begin
      puts "PUBLISH_EVENT"
      b = Bunny.new(:host => APP_CONFIG["eventwire_event_host"], :port =>  APP_CONFIG["eventwire_event_port"], :user =>  APP_CONFIG["eventwire_event_user"], :pass =>  APP_CONFIG["eventwire_event_password"], :vhost =>  APP_CONFIG["eventwire_event_vhost"])
      b.start
      ex = b.exchange(APP_CONFIG["eventwire_event_exchange"], :type => :topic, :persistent => true, :durable => true)
      ex.publish(event_data, :key => event_name.to_s, :persistent => true)
      b.stop
    rescue Bunny::ServerDownError, Bunny::ConnectionError => e
      #If connection to broker is down stop all messages in the loop
      throw
    rescue 
      return false
    end
    puts "END OF publish event"
    return true
    
  end

  def publish_error(error_data = nil)

    begin
      puts "PUBLISH_ERROR"
      b = Bunny.new(:host => APP_CONFIG["eventwire_event_host"], :port =>  APP_CONFIG["eventwire_event_port"], :user =>  APP_CONFIG["eventwire_event_user"], :pass =>  APP_CONFIG["eventwire_event_password"], :vhost =>  APP_CONFIG["eventwire_event_vhost"])
      b.start
      ex = b.exchange(APP_CONFIG["eventwire_event_exchange"], :type => :topic, :persistent => true, :durable => true)
      ex.publish({:data => error_data}.to_json, :key => "event_error", :persistent => true)
      b.stop
    
    rescue Bunny::ServerDownError, Bunny::ConnectionError => e
      #If connection to broker is down stop all messages in the loop
      throw
    rescue
      return false
    end
    puts "END OF publish error event"
    return true

  end
  
  
end
