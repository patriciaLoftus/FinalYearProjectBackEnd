class WeatherParser
  require 'nokogiri'
  require 'net/http'
  def initialize
    uri = URI('http://www.yr.no/place/Ireland/Leinster/Dublin/forecast.xml')
    str = Net::HTTP.get(uri)
    @doc = Nokogiri::XML(str)
  end

  def get_time_slots
    t = @doc.xpath('//tabular')
    t.xpath('time')
  end

  def get_weather(time)
    rain = time.xpath('precipitation/@value').text.to_f
    is_wet?(rain)
  end

  def is_wet?(rain)
    (rain > 0)
  end

  def show
    get_time_slots.each do |time_slot|
      w = get_weather(time_slot)
      puts time_slot
      puts w
    end
  end
  def get_dates(time_slot)
    dates = []
    date_time = get_time_from_string(time_slot.xpath('@from').text)
    dates.push(date_time)
    date_time = get_time_from_string(time_slot.xpath('@to').text)
    dates.push(date_time)
    dates.push(time_slot.xpath('@from'))
    dates
  end

  def get_time_from_string(string)
    string_array = string.split('T')
    date = string_array[0].split('-')
    year = date[0]
    month = date[1]
    day = date[2]
    time = string_array[1].split(':')
    hour = time[0]
    minute = time[1]
    sec = time[2]
    Time.new(year, month,day, hour,minute,sec)
  end

  def weather_at(timestamp)
    time = Time.at(timestamp)
    get_time_slots.each do |time_slot|
      dates = get_dates(time_slot)
      if time.between?(dates[0], dates[1])
        return get_weather(time_slot)
      end
    end
    nil
  end
end
