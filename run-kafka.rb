require 'zk'
require 'yaml'
require 'erb'
require 'json'

module Kafka
  class Broker
    def initialize(heap_size, broker_count, ports, config)
      @heap_size = heap_size.to_i
      @prng = Random.new(Time.now.to_f * 100000)

      @yaml = YAML.load_file(config)

      @zk_servers = @yaml['zk_hosts'].shuffle.join(',')
      @cluster = @yaml['cluster']

      zk_connect

      @ports = ports.split(/,/)

      @broker_count = broker_count.to_i

      @broker_set = get_missing_brokers
      if @broker_set.empty?
        $stderr.puts "No missing brokers found!"
        exit 1
      end

      @attempts = 0
      @max_attempts = 3 * @broker_count

      @hostname = `hostname`.chomp
    end

    def zk_connect
      @zk = ZK.new(@zk_servers, {
        :chroot    => "/kafka-#{@cluster}",
        :thread    => :single,
        :timeout   => 5,
      })

      @zk.wait_until_connected
    end

    def zk_reconnect
      close
      sleep 10
      zk_connect
      become_broker
    end

    # Initially, populate broker set with list of brokers not in Zookeeper, if
    # possible
    def get_missing_brokers
      broker_set = Set.new(0..@broker_count - 1)
      if @zk.exists?('/brokers/ids') && @zk.stat('/brokers/ids').num_children > 0
        ids = @zk.children('/brokers/ids').map{ |x| x.to_i }.sort
        puts "Found these broker IDs in Zookeeper: #{ids}"
        broker_set = broker_set.subtract(ids)
      end
      puts "Missing broker IDs: #{broker_set.to_a.sort}"
      broker_set
    end

    def close
      @candidate.close unless @candidate.nil?
      @zk.close! unless @zk.nil?
    end

    def become_broker
      elected = false
      got_result = false

      @candidate = @zk.election_candidate(
        "kafka-#{@broker_id}", @hostname, :follow => :leader)

      @candidate.on_winning_election {
        puts "Won election for kafka-#{@broker_id}"
        elected = true
        got_result = true

        @zk.on_expired_session do
          puts "ZK session expired"
          zk_reconnect
        end
      }

      @candidate.on_losing_election {
        puts "Lost election for kafka-#{@broker_id}"

        elected = false
        got_result = true
      }

      while !got_result
        puts "Trying to get elected for kafka-#{@broker_id}..."
        @candidate.vote!
        # Random sleep to help avoid thundering herd
        sleep @prng.rand(@broker_count)
        @attempts += 1
        if @attempts > @max_attempts
          break
        end
      end

      if !elected
        close
        $stderr.puts "Couldn't become a broker.  Suiciding."
        exit 1
      end
    end

    def run
      @broker_id = @broker_set.to_a.sample(:random => @prng)
      become_broker


      erb = ERB.new(File.open('server.properties.erb').readlines.map{|x| x.chomp }.join("\n"))
      File.open('server.properties', 'w') do |f|
        f.puts erb.result(binding)
      end

      at_exit {
        close
      }

      env = {
        "KAFKA_HEAP_OPTS" => "-XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Xmx#{@heap_size.to_s}m -Xms#{(@heap_size / 2).to_s}m -XX:NewSize=#{(@heap_size / 3).to_s}m -XX:MaxNewSize=#{(@heap_size / 3).to_s}m -Xss256k -XX:+UseTLAB -XX:+AlwaysPreTouch",
        "SCALA_VERSION" => "2.10.3",
        "KAFKA_LOG4J_OPTS" => "-Dlog4j.configuration=file:log4j.properties",
        "KAFKA_JVM_PERFORMANCE_OPTS" => "-server -XX:+UseCompressedOops -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC",
        "JMX_PORT" => @ports[1],
      }

      %x(tar xf kafka-exec.tar.xz)
      cmd = "./kafka-exec/bin/kafka-run-class.sh -name kafkaServer -loggc kafka.Kafka server.properties".freeze
      last_finished = 0

      loop do
        puts "About to run:"
        puts env, cmd
        GC.start # clean up memory
        system env, cmd
        finished = Time.now.to_f
        if finished - last_finished < 120
          # If the process was running for less than 2 minutes, abort.  We're
          # probably 'bouncing'.  Occasional restarts are okay, but not
          # continuous restarting.
          $stderr.puts "Kafka exited too soon!"
          exit 1
        end
        last_finished = finished
      end
    end
  end
end

begin
  broker = Kafka::Broker.new ARGV[0], ARGV[1], ARGV[2], ARGV[3]
  broker.run
rescue => e
  $stdout.puts $!.inspect, $@
  $stderr.puts $!.inspect, $@
ensure
  broker.close
end
