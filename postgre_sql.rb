class PostgreSQL
  require 'pg'
  require_relative 'queries'
  require_relative 'utils'
  require_relative 'time_predictor'
  require_relative 'bus_data'

public
  def initialize
    @db_name = 'DublinBus'
    @user = 'postgres'
    @password = 'password'
    @host = 'localhost'
    @port = '5433'
    @lines = []
    @q = Queries.new
    connect
  end

  def add_data()
    add_stops
    weather_table
    add_point
    fix_nulls
    make_startend
  end

private
  def weather_table
    File.open('Weather.csv','r').each_line do |line|
      line_arr = line.to_s.split(',')
      date_time  = line_arr[0].split(' ')
      date = date_time[0].split('/')
      wet = false
      if line_arr[1].to_s.to_f > 0
        wet = true
      end
      @con.exec("Insert Into \"Weather\" VALUES ('#{date[1]+'/'+ date[0] + '/' + date[2]}', '#{date_time[1]+':00'}', #{wet})")
    end
  end

  def connect
    begin
      @con = PG.connect :dbname => @db_name, :user => @user, :password => @password, :host => @host, :port => @port
    rescue PG::Error => e
      puts e.message
    end
  end

  def get_all_unique_line_ids
    @con.exec('SELECT DISTINCT "LineID"
    FROM public."BusTimeData"')
  end


  def fix_nulls
    puts Time.new
    nulls = get_nulls
    puts Time.new
    puts "nulls found: #{nulls.num_tuples}"
    nulls.each do |tuple|
      lid = find_lid(tuple)
      if !lid.nil?
        update_all_lids(tuple, lid)
      else
        delete_all_jids(tuple['JourneyID'],tuple['Date'],tuple['VehicleID'])
      end
    end
    puts Time.new
  end

  def delete_all_jids(jid,date,vid)
    @con.exec("DELETE FROM \"BusTimeData\"
              WHERE \"JourneyID\" = #{jid}
              AND \"Date\" = '#{date}'
              AND \"VehicleID\" = #{vid}")
  end

  def update_all_lids(tuple, lid)
    @con.exec("UPDATE \"BusTimeData\"
              SET \"LineID\" = '#{lid}'
              WHERE \"LineID\" = 'null'
              AND \"JourneyID\" = #{tuple['JourneyID']}
              AND \"VehicleID\" = #{tuple['VehicleID']}
              AND \"Date\" = '#{tuple['Date']}'")
  end

  def find_lid(tuple)
    r = @con.exec("SELECT \"LineID\" FROM \"BusTimeData\"
                  WHERE \"LineID\" != 'null'
                  AND \"JourneyID\" = '#{tuple['JourneyID']}' AND \"Date\" = '#{tuple['Date']}'
                  AND \"VehicleID\" = '#{tuple['VehicleID']}'
                  LIMIT 1")
    if r.num_tuples == 1
      r[0]['LineID']
    else
      nil
    end
  end

  def get_nulls
    @con.exec("SELECT DISTINCT ON (\"JourneyID\",\"VehicleID\" ,\"Date\") *
               FROM \"BusTimeData\"
               WHERE \"LineID\" = 'null'")
  end


  def add_to_starts_ends
    puts Time.new
    @con.exec("SELECT
                add_to_starts(\"LineID\", \"JourneyID\",\"Date\"),
                add_to_ends(\"LineID\", \"JourneyID\",\"Date\")
                FROM \"Journeys\"")
    puts Time.new
  end

  def add_start_end
    puts Time.now
    @con.exec("INSERT INTO \"StartEnd\"
              SELECT \"LineID\",\"JourneyID\", \"Date\",
              get_start_time(\"LineID\",\"JourneyID\", \"Date\", get_start_stop(\"LineID\")),
              get_end_time(\"LineID\",\"JourneyID\", \"Date\", get_end_stop(\"LineID\")),\"Day\"
              FROM (
                  SELECT *
                  FROM \"Starts\")
              as journeys")
    puts Time.now
  end

  def fix_bus_route
    errors = get_bus_route_error()
    errors.each do |tuple|
      if bus_route_start_is_right?(tuple['LineID'],(tuple['Start']))
        fix_bus_route_end(tuple['LineID'],tuple['Start'])
      elsif bus_route_end_is_right?(tuple['LineID'],(tuple['Start']))
        fix_bus_route_start(tuple['LineID'],tuple['Start'])
      else
        delete_route(tuple['LineID'])
      end
    end
  end

  def fix_bus_route_start(line,stop)
    @con.exec("Update \"BusRoute\"
              SET \"Start\" = fix_start('#{line}',#{stop})
              WHERE \"LineID\" = '#{line}'")
  end

  def fix_bus_route_end(line,stop)
    @con.exec("Update \"BusRoute\"
              SET \"End\" = fix_end('#{line}',#{stop})
              WHERE \"LineID\" = '#{line}'")
  end

  def get_bus_route_error
    @con.exec("SELECT * FROM \"BusRoute\"
              WHERE \"Start\" = \"End\"")
  end

  def update_start
    puts Time.now
    @con.exec("UPDATE \"StartEnd\"
              SET \"Start\" = get_start_time2(\"LineID\",\"JourneyID\", \"Date\")
              WHERE \"Start\" IS NULL")
    puts Time.now
    @con.exec("UPDATE \"StartEnd\"
              SET \"Start\" = get_start_time3(\"LineID\",\"JourneyID\", \"Date\")
              WHERE \"Start\" IS NULL")
    puts Time.new
  end

  def remove_dummy_journeys
    @con.exec("DELETE FROM \"StartEnd\"
              WHERE \"End\" < \"Start\" OR \"End\" -  \"Start\" <= '0:0:0'")
  end

  def update_end
    puts Time.now
    @con.exec("UPDATE \"StartEnd\"
              SET \"End\" = get_end_time2(\"LineID\",\"JourneyID\", \"Date\")
              WHERE \"End\" IS NULL")
    puts Time.new
  end

  def trim
    puts Time.now
    puts 'trimming journeys'
    @con.exec("SELECT trim_route(\"LineID\",\"Start\",\"End\",\"JourneyID\",\"Date\") FROM \"StartEnd\"")
    puts Time.now
  end

  def add_point
    @con.exec("ALTER TABLE \"BusTimeData\"
               ADD COLUMN \"Point\" geometry")
    @con.exec("UPDATE \"BusTimeData\"
               SET \"Point\" = ST_SetSRID(ST_Point(\"Lon\", \"Lat\"),4326)
               WHERE \"Point\" IS NULL")
  end

  def get_incompletes
    puts Time.now
    puts 'getting incompletes'
    @con.exec("SELECT \"JourneyID\", \"Date\", \"LineID\"
              FROM (
                SELECT DISTINCT ON (\"JourneyID\",\"Date\",\"VehicleID\") *
                FROM \"BusTimeData\"
              ) as journeys
              WHERE is_valid(\"LineID\",\"JourneyID\", \"Date\") = false"
    )
    puts Time.now
  end

  def delete_incompletes
    puts Time.now
    puts 'deleting incompletes'
    result = get_incompletes
    puts result.num_tuples
    puts Time.now
    result.each do |tuple|
      @con.exec("DELETE FROM \"BusTimeData\"
                WHERE \"JourneyID\" = #{tuple['JourneyID']}
                AND \"Date\" = '#{tuple['Date']}'
                AND \"LineID\" = '#{tuple['LineID']}'")
    end
    puts Time.now
  end

  def add_stops
    i = 0
    File.open('stops.txt','r').each_line do |line|
      line_arr = line.split(',')
      stop_str = line_arr[0].to_s
      stop_str = stop_str[7..-2]
      stop_id = stop_str.to_i
      lon = line_arr[2].to_s
      lon = lon[1..-2]
      lat = line_arr[3].to_s
      lat = lat[1..-4]
      name = line_arr[1]
      name = name[1..-1]
      name = remove_apostrophe(name)
      @con.exec("INSERT INTO \"Stops\" VALUES(#{stop_id}, ST_SetSRID(ST_MakePoint(#{lon}, #{lat}),4326),'#{name}')")
    end
  end



  def add_route(text)
    @con.exec("SELECT add_route('#{text}')")
  end

  def add_all_routes
    get_all_unique_line_ids.each do |tuple|
      add_route(tuple['LineID'])
    end
  end

  def make_startend
    delete_incompletes
    add_to_starts_ends
    add_all_routes
    add_start_end
    update_end
    update_start
    delete_from_startend
    trim
  end

  def delete_from_startend
    @con.exec("Delete FROM \"StartEnd\"
    WHERE \"Start\" IS NULL OR \"End\" IS NULL")
  end

  def get_all_main_routes
    r = @con.exec("SELECT substring(\"LineID\"from 1 for 4) FROM \"back_up\"
                  WHERE substring(\"LineID\" from 8 for 1) = '1'
                LIMIT 10")
    lines = []
    r.each do |t|
      line = t['substring']
      remove_zeros(line)
      lines.push(remove_zeros(line))
    end
  end

  def bus_route_start_is_right?(line, stop)
    res = count_starts_and_ends(line, stop)
    res[0]['count_starts'].to_i  > res[0]['count_ends'].to_i
  end

  def bus_route_end_is_right?(line, stop)
    res = count_starts_and_ends(line, stop)
    res[0]['count_ends'].to_i > res[0]['count_starts'].to_i
  end

  def delete_route(line)
    @con.exec("DELETE FROM \"BusRoute\" WHERE \"LineID\" = '#{line}'")
  end

  def count_starts_and_ends(line, stop)
    @con.exec("SELECT count_starts('#{line}',#{stop}), count_ends('#{line}',#{stop})")
  end

  def add_to_routes
    @con.exec(@q.add_routes)
  end

  def get_minutes_from(arr)
    minutes = []
    arr.each do |time|
      if time.nil?
        minutes.push(-1)
        next
      end
      minute = 0.0
      time_arr = time.split(':')
      minute += time_arr[0].to_i * 60
      minute += time_arr[1].to_i
      minute += time_arr[2].to_f/60.0
      minute = minute.round(2)
      minutes.push(minute)
    end
    minutes
  end

end