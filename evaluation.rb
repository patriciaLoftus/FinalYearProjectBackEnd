class Evaluation
  require 'pg'
  require_relative 'queries'
  require_relative 'utils'
  require_relative 'time_predictor'
  def initialize
    @db_name = 'DublinBus'
    @user = 'postgres'
    @password = 'password'
    @host = 'localhost'
    @port = '5433'
    @lines = []
    @q = Queries.new
    @table_name = "\"TestData\""
    @start_end = "\"TestStartEnd\""
    @output_fname = 'EvalOutput.csv'
    @t = TimePredictor.new
    connect
  end
  def connect
    begin
      @con = PG.connect :dbname => @db_name, :user => @user, :password => @password, :host => @host, :port => @port
    rescue PG::Error => e
      puts e.message
    end
  end

  def full_route_eval()
    puts Time.new()
    percentages = []
    percentages.push(0.0)
    percentages.push(0.0)
    percentages.push(0.0)
    percentages.push(0.0)
    percentages.push(0.0)
    percentages.push(0.0)
    test_journeys = @con.exec("
      SELECT *,\"End\" - \"Start\" as journey_time ,\"Start\" + interval '15 minutes' AS max_time,
      \"Start\" - interval '15 minutes' AS min_time, get_weather(\"Start\") as weather
      FROM \"Test\"")
    puts test_journeys.num_tuples
    i = 0
    test_journeys.each do |journey|
      t = Time.now
      journeys = @t.get_journeys_full_route(journey['min_time'],journey['max_time'],journey['LineID'],journey['Day'])
      r = @t.get_weighted_avg_time(journeys, journey['Start'])
      actual = Utils.new.interval_to_minutes(journey['journey_time'])
      next if r.nil?
      next if !r.has_key?(journey['LineID'])

      percentages[0] += (r[journey['LineID']] - actual).abs/actual

     r = @t.get_weighted_avg_time_weather(journeys,journey['weather'],journey['Start'])


      percentages[1] += (r[journey['LineID']] - actual).abs/actual

      r = @t.get_weighted_avg_weather(journeys,journey['weather'])

      percentages[2] += (r[journey['LineID']] - actual).abs/actual
      t = Time.now
      r = @t.get_avg_full(journeys, journey['weather'])
      t2 = Time.now
      percentages[3]+=(t - t2).abs
      percentages[3] += (r[journey['LineID']] - actual).abs/actual
      i+=1

    end
    puts Time.new()
    puts "number of predictions made for weighted & non-weather #{i}"
    puts  "Avg: #{percentages[3]}"
    puts  "Avg Weather: #{percentages[2]}"
    puts  "Avg time: #{percentages[0]}"
    puts  "Avg time & Weather: #{percentages[1]}"
  end

  def get_percentage_error(results)
    total_results = []
    total_results.push(0.0)
    total_results.push(0.0)
    total_results.push(0.0)
    total_results.push(0.0)
    total_percentage_error = []
    total_percentage_error.push(0.0)
    total_percentage_error.push(0.0)
    total_percentage_error.push(0.0)
    total_percentage_error.push(0.0)
    u = Utils.new
    total_result = 0
    total_actual = 0.0
    results.each do |result|
      actual = u.interval_to_minutes(result['difference'])
      next if actual <= 0
      total_actual += actual
      line_id = result['LineID']
      start_stop = result['start_stop']
      end_stop = result['end_stop']
      day = result['Day']
      start_time = result['start_time']
      weather = result['weather']
      min = result['min_time']
      max = result['max_time']
      predictions = @t.get_predictions(start_stop,end_stop,day,start_time, weather, min, max)
      i = 0
      predictions.each do |prediction|
        next if prediction.nil?
        predicted = prediction[line_id]
        next if predicted.nil?
        percentage_error = (predicted-actual).abs/actual
        total_percentage_error[i]+= percentage_error
        total_results[i]+=1
        i+=1
      end
      total_result+=1
    end
    puts "actual average #{total_actual/total_result}"
    puts "error weather #{total_percentage_error[0]/total_results[0]}"
    puts "error time #{total_percentage_error[1]/total_results[1]}"
    puts "error weather & time #{total_percentage_error[2]/total_results[2]}"
    puts "error non weighted #{total_percentage_error[3]/total_results[3]}"

    puts Time.new
  end

  def stop_to_stop_evaluation
    puts Time.new
    i = 0
    results = @con.exec("
    SELECT (end_time - start_time) as difference, CAST(start_time AS time) as start_time,
    get_weather(start_time) as weather, CAST(start_time + interval '15 minutes' AS time) as max_time,
    CAST(start_time - interval '15 minutes' AS time) as min_time, \"LineID\", \"Day\", start_stop, end_stop
    FROM (
      SELECT *, time_at(end_stop, \"JourneyID\", \"LineID\", \"Date\") as end_time FROM (
            SELECT *, random_stop_after(\"JourneyID\", \"LineID\", \"Date\", start_time) as end_stop FROM (
                SELECT * ,time_at(start_stop,\"JourneyID\", \"LineID\", \"Date\") as start_time FROM (
                    SELECT *,  random_stop(\"JourneyID\", \"LineID\", \"Date\") as start_stop FROM
                    \"Test\"
                    ORDER BY RANDOM()
                    LIMIT 100
                ) as start_stops
            ) as start_times
        ) as end_stop
    ) as end_times")
    percentages = []
    percentages.push(0.0)
    percentages.push(0.0)
    percentages.push(0.0)
    percentages.push(0.0)
    puts results.num_tuples
    results.each do |tuple|
      journeys = @t.get_journeys(tuple['start_time'],tuple['min_time'],tuple['max_time'],tuple['Day'],tuple['start_stop'],
      tuple['end_stop'])
      next if journeys.nil?
      flag = false
      journeys.each do |j|
        if j['LineID'] = tuple['LineID']
          flag = true
          break
        end
      end
      next if !flag
      actual = Utils.new.interval_to_minutes(tuple['difference'])
      next if actual <= 0
      r = @t.get_avg_full(journeys, tuple['weather'])
      next if r.nil?
      next if r[tuple['LineID']].nil?
      percentages[0] += (r[tuple['LineID']] - actual).abs/actual

      r = @t.get_weighted_avg_time(journeys, tuple['start_time'])

      percentages[1] += (r[tuple['LineID']] - actual).abs/actual

      r = @t.get_weighted_avg_time_weather(journeys,tuple['weather'], tuple['start_time'])
      percentages[2] += (r[tuple['LineID']] - actual).abs/actual

      r = @t.get_weighted_avg_weather(journeys, tuple['weather'])
      percentages[3] += (r[tuple['LineID']] - actual).abs/actual
      i+=1
    end

    puts Time.new()
    puts "number of predictions made for weighted & non-weather #{i}"
    puts  "Avg: #{percentages[0]/i}"
    puts  "Avg Weather: #{percentages[3]/i}"
    puts  "Avg time: #{percentages[1]/i}"
    puts  "Avg time & Weather: #{percentages[2]/i}"
  end
end
e = Evaluation.new
e.stop_to_stop_evaluation()