require 'pg'
class DataM
  def initialize
    @db_name = 'DublinBus'
    @user = 'postgres'
    @password = 'ballina096'
    @host = 'localhost'
    @port = '5433'
    connect
  end
  def connect
    begin
      @con = PG.connect :dbname => @db_name, :user => @user, :password => @password, :host => @host, :port => @port
    rescue PG::Error => e
      puts e.message
    end
  end
  def get_databases(name)
   f = File.new(name + '.csv', 'a')
    @con.copy_data("COPY \"#{name}\" TO STDOUT CSV") do
      while row= @con.get_copy_data
        f.puts(row)
      end
    end
  end
end

DataM.new.get_databases("BusTimeData")