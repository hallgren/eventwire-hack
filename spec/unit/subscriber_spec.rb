require 'spec_helper'

describe Eventwire::Subscriber do
  
  describe '#on' do
    
    subject { class_including Eventwire::Subscriber }

    it 'should subscribe to the event using the current driver' do
      Eventwire.driver.should_receive(:subscribe).with(:task_completed, anything)
      
      subject.on(:task_completed) { } 
    end
    
    it 'should subscribe with a handler that builds an event object' do
      Eventwire.driver.should_receive(:subscribe) do |event_name, _, &handler|
        handler.call(:task_name => 'Cleaning')
        handler.call('task_name' => 'Cleaning')
      end
      
      subject.on(:task_completed) { |data| data.task_name.should == 'Cleaning' }
    end
    
    it 'should subscribe to the event with incremental handler ids based on the class name' do
      ThisModule = Module.new
      ThisModule::ThisSubscriber = subject
      
      Eventwire.driver.should_receive(:subscribe).with(:task_completed, 'ThisModule::ThisSubscriber::1')
      Eventwire.driver.should_receive(:subscribe).with(:task_completed, 'ThisModule::ThisSubscriber::2')
      
      2.times do
        subject.on(:task_completed) { } 
      end
      
      ThisModule.parent.send :remove_const, :ThisModule
    end
    
    describe 'Error handling' do
          
      it 'should subscribe with a handler that is fault tolerant' do
        Eventwire.driver.should_receive :subscribe do |event_name, _, &handler| 
          handler.call
        end
      
        subject.on :task_completed do
          raise 'This exception should be catched'
        end
      end
      
      it 'should run on_error block if present' do
        error = nil
        
        Eventwire.on_error do |e| 
          error = e
        end
        
        Eventwire.driver.should_receive :subscribe do |event_name, _, &handler| 
          handler.call
        end
      
        subject.on(:task_completed) { raise 'error!' }
        
        error.message.should == 'error!'
      end
      
      it 'should subscribe with a handler that logs when an exception happens' do
        io = StringIO.new
        Eventwire.logger = Logger.new(io)
        
        Eventwire.driver.should_receive :subscribe do |event_name, _, &handler| 
          handler.call
        end
      
        subject.on(:task_completed) { raise 'This exception should be logged' }
        error_backtrace = "#{__FILE__}:#{__LINE__-1}"
        
        io.string.should include('This exception should be logged')
        io.string.should include(error_backtrace)
      end
    
    end
    
    describe 'Logging' do
      
      it 'should subscribe with a handler that logs the start and end of event processing' do
        ThisSubscriber = subject
        io = StringIO.new
        Eventwire.logger = Logger.new(io)
        
        Eventwire.driver.should_receive :subscribe do |event_name, _, &handler| 
          handler.call :task_name => 'Cleaning'
        end
      
        subject.on(:task_completed) { io.puts 'Hello from the handler' }
        
        io.string.should == <<-OUTPUT
Starting to process `task_completed` with handler `ThisSubscriber::1` and data `{:task_name=>"Cleaning"}`
Hello from the handler
End processing `task_completed`
OUTPUT
      end
      
      it 'should subscribe with a handler that logs even when an exception occur' do
        io = StringIO.new
        Eventwire.logger = Logger.new(io)
        
        Eventwire.driver.should_receive :subscribe do |event_name, _, &handler| 
          handler.call
        end
      
        subject.on(:task_completed) { raise }
        
        io.string.should include('Starting to process')
        io.string.should include('End processing')
      end
      
    end
    
  end
  
end