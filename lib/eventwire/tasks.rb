namespace :eventwire do
  task :work do
    trap('INT') { Eventwire.stop_worker; exit }
    trap('TERM') { Eventwire.stop_worker; exit }
    Eventwire.start_worker
  end

  task :work_send do
    trap('INT') { Eventwire.stop_worker; exit }
    trap('TERM') { Eventwire.stop_worker; exit }
    Eventwire.start_worker_send
  end
end
