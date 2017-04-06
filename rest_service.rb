require 'sinatra'
require 'sinatra/cross_origin'
require 'JSON'
require_relative 'time_predictor'
require_relative 'utils'
class RestService
  def initialize
    @predictor = TimePredictor.new
  end

  def remove_zeros(line)
    if line.split('')[0] == '0'
      remove_zeros(line.slice(1, line.length))
    else
      line
    end
  end

  def format_readable(line)
    line = remove_zeros(line)
    l = line.length
    l -= 4
    line.slice(0, l)
  end

  def format(str)
    str_new = ''
    str.split('').each do |char|
      if char != "\\"
        str_new += char
      end
    end
    str_new
  end

  def get_average_time(origin, destination,time)
    res = @predictor.get_avg(origin, destination,time)
    return nil if res.empty?
    check_for_duplicates(res)
  end

  def to_minutes(time)
    minute = 0.0
    time_arr = time.split(':')
    minute += time_arr[0].to_i * 60
    minute += time_arr[1].to_i
    minute += time_arr[2].to_f/60.0
    minute.round(2)
  end

  def check_for_duplicates(line_times)
    lines = Hash.new()
    line_times.each_key do |line|
      line_readable = format_readable(line)
      if lines.has_key?(line_readable)
        time = lines[line_readable]
        if time.to_s.include?(' - ')
          arr = time.split(' - ')
          if arr[0].to_f <= line_times[line].to_f
            if arr[1].to_f < line_times[line].to_f
              lines[line_readable] = "#{arr[0]} - #{line_times[line]}"
            end
          else
            lines[line_readable] = "#{line_times[line]} - #{arr[1]}"
          end
        else
          if time.to_f < line_times[line].to_f
            lines[line_readable] = "#{time} - #{line_times[line]}"
          elsif time.to_f > line['time'].to_f
            lines[line_readable] = "#{line_times[line]} - #{time}"
          end
        end
      else
        lines.store(line_readable,line_times[line])
      end
    end
    json_array = []
    lines.each_key() do |id|
      json = {}
      json['id'] = id
      json['time'] = lines[id].to_f.round(2)
      json_array.push(json)
    end
    json_array.sort{|e1,e2|e1['time'] <=> e2['time']}
  end

  def valid_route?(origin,destination)
    TimePredictor.new.has_route_between(origin,destination)
  end


  def get_previous_and_next_journeys(origin, destination,time)
    t = Time.at(time)
    day = t.wday
    if day == 0
      day = 7
    end
    @predictor.get_prev_and_next_journeys(origin, destination,time,day)
  end

  def get_stops
    stops = []
    @predictor.get_stops.each do |tuple|
      stop = {}
      stop['value'] = tuple['StopID']
      stop['label'] = tuple['Name'].chop! + ', ' + tuple['StopID']
      stops.push(stop)
    end
    stops
  end

  def get_stops_position
    TimePredictor.new.get_stops_position
  end

  def get_times_for_graph(timestamp, origin,destination)
    times = []
    u = Utils.new
    TimePredictor.new.get_average_for_the_day(timestamp, origin,destination).each do |avg|
      mins = u.interval_to_minutes(avg)
      times.push(mins)
    end
    times
  end

end
rest = RestService.new

configure do
  enable :cross_origin
end

enable :cross_origin
set :port, 9292

post '/getstopsandpositions' do
  message = {}
  message['options'] = rest.get_stops_position
  message.to_json
end


post '/getstops' do
  message = {}
  message['options'] = rest.get_stops
  message.to_json
end

post '/graph' do
  object = {}
  request.body.rewind
  data = JSON.parse request.body.read()
  if data.has_key?('timestamp') && data.has_key?('origin') && data.has_key?('destination')
    timestamp = data['timestamp'].to_i
    origin = data['origin'].to_i
    destination = data['destination'].to_i
    if rest.valid_route?(origin,destination)
    puts 'graph has params'
    object['error'] = 'null'
    values = rest.get_times_for_graph(timestamp,origin,destination)
    object['values'] = values
    puts values
    else
      object['error'] = 'invalid route'
    end
  else
    puts 'graph no params'
    object['error'] = 'missing params'
  end
  object.to_json
end

post '/api' do
  return_message = {}
  request.body.rewind
  data = JSON.parse request.body.read()
  puts data
  if data.has_key?('timestamp') && data.has_key?('origin') && data.has_key?('destination')
    timestamp = data['timestamp'].to_i
    origin = data['origin'].to_i
    destination = data['destination'].to_i
    if rest.valid_route?(origin,destination)
      time = rest.get_average_time(origin, destination,timestamp)
      if time != nil
        return_message['error'] = 'null'
        return_message['time'] = time
      else
        return_message['error'] = 'no journey found'
        t = Time.at(timestamp)
        journeys = rest.get_previous_and_next_journeys(origin, destination,t)
        return_message['prev'] = journeys[0]
        return_message['next'] = journeys[1]
      end
    else
      return_message['error'] ='invalid route'
    end
  else
    return_message['error'] = 'data missing'
  end
  return_message.to_json
end

options '*' do
end

