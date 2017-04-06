class Utils
  def interval_to_minutes(interval)
    if interval == nil
      return 0
    end
    time_array = interval.split(':')
    minutes = 0.0
    minutes += time_array[0].to_i * 60
    minutes += time_array[1].to_i
    minutes += (time_array[2].to_i)/60.0
    minutes
  end
end