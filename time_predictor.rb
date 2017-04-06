class TimePredictor
  require_relative 'queries'
  require_relative 'time_string'
  require 'pg'
  require_relative 'weather_parser'
  require_relative 'utils'
  def initialize
    @db_name = 'DublinBus'
    @user = 'postgres'
    @password = 'password'
    @host = 'localhost'
    @port = '5433'
    @q = Queries.new
    @weather_used = nil
    @weather = nil
    connect
  end

  def connect
    begin
      @con = PG.connect :dbname => @db_name, :user => @user, :password => @password, :host => @host, :port => @port
    rescue PG::Error => e
      puts e.message
    end
  end

  def get_average_for_the_day(timestamp, origin, destination)
    t = Time.at(timestamp)
    day = get_day(t)
    results = []
    i = 6
    while i < 24
      result = get_average_time_for_hour(day, origin, destination, i)
      results.push(result)
      i+=1
    end
    results
  end

  def get_average_time_for_hour(day, origin, destination, i)
    now = Time.new()
    min = Time.new(now.year, now.month, now.day, i, 0 ,0)
    max = Time.new(now.year, now.month, now.day, i, 59, 59)
    r = @con.exec(@q.get_average_time_for_hour(day, origin, destination, min, max))
    r[0]['avg']
  end

  def get_times_within(time, mins)
    time_before = time - (mins *  60)
    time_after = time + (mins * 60)
    [time_before, time_after]
  end

  def get_average_time(time, day, line_id)
    times = get_times_within(time, 15)
    return nil if times.nil?
    time_before = times[0]
    time_after = times[1]
    if time_before.day != time_after.day
      if time.day == time_before.day
        day1 = day-1
        day2 = day
      else
        day1 = day
        day2 = day+1
      end
      select =  @con.exec("SELECT * FROM \"StartEnd\"
                WHERE \"LineID\" = #{line_id}
                AND ((\"Day\" = #{day1}
                AND CAST(\"Start\" as time) >= CAST('#{time_before}' as time)
                AND CAST(\"Start\" as time) <= '23:59:59')
                OR (\"Day\" = #{day2}
                AND CAST(\"Start\" as time) >= '00:00:00'
                AND CAST(\"Start\" as time) <= CAST('#{time_after}' as time)))")
    else
      select = "SELECT * FROM \"StartEnd\"
                WHERE \"LineID\" = '#{line_id}'
                AND \"Day\" = #{day}
                AND CAST(\"Start\" as time) >= CAST('#{time_before}' as time)
                AND CAST(\"Start\" as time) <= CAST('#{time_after}' as time)"
    end
    s = @con.exec(select)
    s.each do  |t|
      puts t
    end
    puts '*************'
    r= @con.exec("SELECT AVG(\"End\" - \"Start\") FROM (
               #{select} ) as journeys")
    r.each do |t|
      puts t
    end
  end

  def remove_apostrophe(name)
    arr = name.split('')
    str = ''
    arr.each do |letter|
      if letter == "'"
        str += "''"
      else
        str +=letter
      end
    end
    str
  end

  def get_weather
    @weather
  end

  def get_route
    '046A0001'
  end

  def get_time
    Time.new
  end

  def get_day(time)
    day = time.wday
    if day == 0
      day = 7
    end
    day
  end

  def add_zeros(line)
    if line.length == 8
      line
    else
      line = '0' + line
      add_zeros(line)
    end
  end

  def find_directions(line, direction)
    line += direction.to_s + '001'
    line = add_zeros(line)
    @con.exec("SELECT \"Name\"
              FROM \"BusRoute\"
              INNER JOIN \"Stops\" ON (\"BusRoute\".\"End\" = \"Stops\".\"StopID\")
              WHERE \"BusRoute\".\"LineID\" = '#{line}'")
  end

  def get_avg(origin, destination, time)
    t = Time.at(time)
    times = get_times_within(t, 15)
    day = get_day(t)
    w = WeatherParser.new
    wet = w.weather_at(time)
    @weather = wet
    journeys = get_journeys(time, times[0], times[1],day,origin, destination)
    avg = get_weighted_avg_time_weather(journeys,wet,time)
  end


  def has_route_between(origin, destination)
    r = @con.exec("SELECT get_lines_with_stops(#{origin},#{destination})")
    (r.ntuples > 0)
  end

  def get_prev_and_next_journeys(origin, destination,time,day)
    r = @con.exec(@q.get_prev(origin, destination,time,day))
    r1 = @con.exec(@q.get_next(origin, destination,time,day))
    [r[0]['time'], r1[0]['time']]
  end

  def get_stops
    @con.exec("SELECT * FROM \"Stops\"")
  end

  def get_journeys_full_route(time_min, time_max, line_id, day)
    if time_min < time_max
    @con.exec("
      SELECT *,Cast(\"Start\" as time) as start_time, \"End\" - \"Start\" as journey_time, get_weather(\"Start\") as weather
      FROM \"StartEnd\"
      WHERE CAST(\"Start\" as time ) BETWEEN '#{time_min}' AND '#{time_max}'
      AND \"Day\" = #{day} AND \"LineID\" = '#{line_id}' -- AND \"Date\" > '2012-12-20'")
    else
      @con.exec("
      SELECT *, \"End\" - \"Start\" as journey_time, get_weather(\"Start\") as weather
      FROM \"StartEnd\"
      WHERE ((CAST(\"Start\" as time) BETWEEN '#{time_min}' AND '11:59:59' AND \"Day\" = #{day})
       OR (CAST(\"Start\" as time ) BETWEEN '00:00:00' AND '#{time_max}' AND \"Day\" = #{day+1})
      AND \"LineID\" = '#{line_id}' -- AND \"Date\" > '2012-12-20'")
    end
  end

  def get_weighted_avg_weather(journeys,weather)
    total = 0.0
    total_weight = 0.0
    result_avg = {}
    result = journeys
    line_id = nil
    if result.num_tuples == 0
      return nil
    end
      result.each do |tuple|
        if line_id != tuple['LineID']
          if (!line_id.nil?)
            avg = total/total_weight
            result_avg.store(line_id, avg)
          end
          total = 0.0
          total_weight = 0.0
          line_id = tuple['LineID']
        end
        if tuple['weather'].to_s == weather.to_s.split('')[0]
          w = 100
        else
          w = 0.1
        end
        total += Utils.new.interval_to_minutes(tuple['journey_time']) * w
        total_weight+=w
      end
    avg = total/total_weight
    result_avg.store(line_id, avg)
    result_avg
  end

  def get_full_wo_weather_line(journeys,line)
    avg = 0.0
    i = 0
    journeys.each do |j|
      next if j['LineID']!= line
      avg+= Utils.new.interval_to_minutes(j['journey_time'])
      i+=1
    end
    if i == 0
      return nil
    end
    avg/i
  end

  def get_weighted_avg_time(journeys,time)
    time_actual = TimeString.new(time)
    total = 0.0
    total_weight = 0.0
    result_avg = {}
    result = journeys
    line_id = nil
    if result.num_tuples == 0
      return nil
    end
    min_w  = 10000
    max_w = 0
    result.each do |tuple|
      time_actual.revert
      time_journey = TimeString.new(tuple['start_time'])
      time_actual.add_day_to(time_journey)
      w = time_actual.absolute_difference(time_journey)
      if w < min_w
        min_w = w
      end
      if w > max_w
        max_w = w
      end
    end
    result.each do |tuple|
      time_actual.revert
      time_journey = TimeString.new(tuple['start_time'])
      time_actual.add_day_to(time_journey)
      w = 2 - (time_actual.absolute_difference(time_journey) - min_w)/(max_w-min_w)
      if line_id != tuple['LineID']
        if (!line_id.nil?)
          avg = total/total_weight
          result_avg.store(line_id, avg)
        end
        total = 0.0
        total_weight = 0.0
        line_id = tuple['LineID']
      end
      total += Utils.new.interval_to_minutes(tuple['journey_time']) * w
      total_weight+=w
    end
    avg = total/total_weight
    result_avg.store(line_id, avg)
    result_avg
  end

  def get_avg_full(journeys,weather)
    result = get_avg_full_with_weather(journeys,weather)
    result.each_key do |line|
      if result[line].nil?
        avg = get_full_wo_weather_line(journeys, line)
        result.store(line, avg)
      end
    end
  end

  def get_avg_full_with_weather(journeys,weather)
    total = 0.0
    total_weight = 0.0
    result = journeys
    line_id = nil
    result_avg = {}
    result.each do |tuple|
      if line_id != tuple['LineID']
        if (!line_id.nil?)
          if (total_weight != 0.0)
            avg = total/total_weight
            result_avg.store(line_id, avg)
          else
            result_avg.store(line_id, nil)
          end
        end
        total = 0.0
        total_weight = 0.0
        line_id = tuple['LineID']
      end
      if(tuple['weather'] == weather.split('')[0])
        total += Utils.new.interval_to_minutes(tuple['journey_time'])
        total_weight+= 1
      end
    end
    if (total_weight != 0.0)
      avg = total/total_weight
      result_avg.store(line_id, avg)
    else
      result_avg.store(line_id, nil)
    end
    result_avg
  end

  def get_full_without_weather(journeys)
    result_avg = {}
    total = 0.0
    total_weight = 0.0
    result = journeys
    line_id = nil
    result.each do |tuple|
      if line_id != tuple['LineID']
        if (!line_id.nil?)
          if (total_weight != 0.0)
            avg = total/total_weight
            result_avg.store(line_id, avg)
          else
            result_avg.store(line_id, nil)
          end
        end
        total = 0.0
        total_weight = 0.0
        line_id = tuple['LineID']
      end
        total += Utils.new.interval_to_minutes(tuple['journey_time'])
        total_weight+= 1
    end
    avg = total/total_weight
    result_avg.store(line_id, avg)
    result_avg
  end

  def get_weighted_avg_time_weather(journeys,weather,time)
    time_actual = TimeString.new(time.to_s)
    total = 0.0
    total_weight = 0.0
    result_avg = {}
    result = journeys
    line_id = nil
    if result.num_tuples == 0
      return nil
    end
    min_w  = 15*61
    max_w = -1
    lines = {}
    result.each do |tuple|
      if lines.has_key?(tuple['LineID'])
        i = lines[tuple['LineID']]
        lines.store(tuple['LineID'], i+1)
      else
        lines.store(tuple['LineID'], 1)
      end
      time_actual.revert
      time_journey = TimeString.new(tuple['start_time'])
      time_actual.add_day_to(time_journey)
      w = time_actual.absolute_difference(time_journey)
      if w < min_w
        min_w = w
      end
      if w > max_w
        max_w = w
      end
    end
    result.each do |tuple|
      time_actual.revert
      time_journey = TimeString.new(tuple['start_time'])
      time_actual.add_day_to(time_journey)
      w = 2 - (time_actual.absolute_difference(time_journey) - min_w)/(max_w-min_w)
      if(tuple['weather'] == weather.to_s.split('')[0])
        w += w/1000
      end
      if line_id != tuple['LineID']
        if (!line_id.nil?)
          avg = total/total_weight
          puts "#{line_id} : #{avg}"
          result_avg.store(line_id, avg)
        end
        total = 0.0
        total_weight = 0.0
        line_id = tuple['LineID']
      end
      total += Utils.new.interval_to_minutes(tuple['journey_time']) * w
      total_weight+=w
    end
    avg = total/total_weight
    result_avg.store(line_id, avg)
    result_avg
  end

  def get_stops_position
    stops = []
    r = @con.exec("SELECT ST_X(\"Point\") as x, ST_Y(\"Point\") as y, \"StopID\", \"Name\"
                  FROM \"Stops\"")
    r.each do |stop|
      json = {}
      json['lat'] = stop['x']
      json['lon'] = stop['y']
      json['id'] = stop['StopID']
      json['name'] = stop['Name']
      stops.push(json)
    end
    stops
  end


  def get_journey_times(time_min, time_max, day, origin,destination)
    if time_min > time_max
      result = @con.exec("
      WITH
        starts AS (
        SELECT Min(\"Timestamp\") as start_time, \"LineID\" ,\"JourneyID\", \"Date\" FROM (
          SELECT *
          FROM \"BusTimeData\"
          WHERE  \"StopID\" = #{origin} AND ((
              CAST(\"Timestamp\" AS TIME)  BETWEEN '#{time_min}' AND '23:59:59}'
              AND \"Day\"= #{day}
          )
          OR (
              CAST(\"Timestamp\" AS TIME)  BETWEEN '00:00:00' AND '#{time_max}'
              AND \"Day\" = #{day.to_i + 1}
          ))
          ) as starts
        GROUP BY (\"LineID\",\"JourneyID\", \"Date\")
      ),
     ends AS (
      SELECT Min(\"Timestamp\") as end_time, \"LineID\" ,\"JourneyID\", \"Date\" FROM (
        SELECT *
        FROM \"BusTimeData\"
        WHERE  \"StopID\" = #{destination} ) as ends
      GROUP BY (\"LineID\",\"JourneyID\", \"Date\")
    ),
    lines AS (
       SELECT get_lines_with_stops(#{origin},#{destination}) as line_id
    )
    SELECT (ends.\"end_time\" - starts.\"start_time\") as journey_time, get_weather(starts.\"start_time\") as weather,
    ends.\"LineID\",CAST(starts.\"start_time\" as time)
    FROM starts Natural JOIN ends JOIN lines ON
       (ends.\"LineID\" = lines.line_id)
    ORDER BY  \"LineID\" ASC
  ")
    else
      result = @con.exec("
      WITH
        starts AS (
        SELECT Min(\"Timestamp\") as start_time, \"LineID\" ,\"JourneyID\", \"Date\" FROM (
          SELECT *
          FROM \"BusTimeData\"
          WHERE  \"StopID\" = #{origin} AND
              CAST(\"Timestamp\" AS TIME)  BETWEEN '#{time_min}' AND '#{time_max}'
              AND \"Day\"= #{day}) as starts
        GROUP BY (\"LineID\",\"JourneyID\", \"Date\")
      ),
     ends AS (
      SELECT Min(\"Timestamp\") as end_time, \"LineID\" ,\"JourneyID\", \"Date\" FROM (
        SELECT *
        FROM \"BusTimeData\"
        WHERE  \"StopID\" = #{destination} ) as ends
      GROUP BY (\"LineID\",\"JourneyID\", \"Date\")
    ),
    lines AS (
       SELECT get_lines_with_stops(#{origin},#{destination}) as line_id
    )
    SELECT (ends.\"end_time\" - starts.\"start_time\") as journey_time, get_weather(starts.\"start_time\") as weather,
    ends.\"LineID\",CAST(starts.\"start_time\" as time)
    FROM starts Natural JOIN ends JOIN lines ON
       (ends.\"LineID\" = lines.line_id)
    ORDER BY  \"LineID\" ASC")
    end
    result
  end

  def get_journeys(time, time_min, time_max, day, origin,destination)
    if time_min > time_max
      result = @con.exec("
      WITH
        starts AS (
        SELECT Min(\"Timestamp\") as start_time, \"LineID\" ,\"JourneyID\", \"Date\" FROM (
          SELECT *
          FROM \"BusTimeData\"
          WHERE  \"StopID\" = #{origin} AND ((
              CAST(\"Timestamp\" AS TIME)  BETWEEN '#{time_min}' AND '23:59:59}'
              AND \"Day\"= #{day}
          )
          OR (
              CAST(\"Timestamp\" AS TIME)  BETWEEN '00:00:00' AND '#{time_max}'
              AND \"Day\" = #{day.to_i + 1}
          ))
          ) as starts
        GROUP BY (\"LineID\",\"JourneyID\", \"Date\")
      ),
     ends AS (
      SELECT Min(\"Timestamp\") as end_time, \"LineID\" ,\"JourneyID\", \"Date\" FROM (
        SELECT *
        FROM \"BusTimeData\"
        WHERE  \"StopID\" = #{destination} ) as ends
      GROUP BY (\"LineID\",\"JourneyID\", \"Date\")
    ),
    lines AS (
       SELECT get_lines_with_stops(#{origin},#{destination}) as line_id
    )
    SELECT (ends.\"end_time\" - starts.\"start_time\") as journey_time, get_weather(starts.\"start_time\") as weather
    FROM starts Natural JOIN ends JOIN lines ON
       (ends.\"LineID\" = lines.line_id)
    ORDER BY  \"LineID\" ASC
  ")
    else
      result = @con.exec("
      WITH
        starts AS (
        SELECT Min(\"Timestamp\") as start_time, \"LineID\" ,\"JourneyID\", \"Date\" FROM (
          SELECT *
          FROM \"BusTimeData\"
          WHERE  \"StopID\" = #{origin} AND
              CAST(\"Timestamp\" AS TIME)  BETWEEN CAST('#{time_min}' AS TIME) AND CAST('#{time_max}' AS TIME)
              AND \"Day\"= #{day}) as starts
        GROUP BY (\"LineID\",\"JourneyID\", \"Date\")
      ),
     ends AS (
      SELECT Min(\"Timestamp\") as end_time, \"LineID\" ,\"JourneyID\", \"Date\" FROM (
        SELECT *
        FROM \"BusTimeData\"
        WHERE  \"StopID\" = #{destination} ) as ends
      GROUP BY (\"LineID\",\"JourneyID\", \"Date\")
    ),
    lines AS (
       SELECT get_lines_with_stops(#{origin},#{destination}) as line_id
    )
    SELECT (ends.\"end_time\" - starts.\"start_time\") as journey_time, get_weather(starts.\"start_time\") as weather,
    ends.\"LineID\",CAST(starts.\"start_time\" as time)
    FROM starts Natural JOIN ends JOIN lines ON
       (ends.\"LineID\" = lines.line_id)
    ORDER BY  \"LineID\" ASC")
    end
    result
  end
end