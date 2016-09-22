module Fluent
  require_relative 'omslog'
  require_relative 'oms_common'
  require 'json'
  require 'open3'


  class HdinsightFilter < Filter
    Fluent::Plugin.register_filter('filter_hdinsight', self)

    BASE_DIR = File.dirname(File.expand_path('..', __FILE__))
    RUBY_DIR = BASE_DIR + '/ruby/bin/ruby '
    SCRIPT = BASE_DIR + '/bin/hdinsightmanifestreader.rb '

  def configure(conf)
      super
      @hostname = OMS::Common.get_hostname or "Unknown host"
      @command = "sudo " << RUBY_DIR << SCRIPT
      @clustername = ""
      Open3.popen3(@command) {|stdin, stdout, stderr, wait_thr|
          pid = wait_thr.pid
          stdin.close
          @clustername = stdout.read
          wait_thr.value
      }
    end

    def start
      super
    end

    def shutdown
      super
    end

    def filter(tag, time, record)
      record["ClusterName"] = @clustername
      record["HostName"] = @hostname
      record
    end
  end
end
