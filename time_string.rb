class TimeString
  def initialize(time)
    time_array = time.split(':')
    @hours = time_array[0].to_i
    @minutes = time_array[1].to_i
    @seconds = time_array[2].to_f
  end

  def to_seconds
    s = @seconds
    s += @minutes * 60
    s += @hours * 60 * 60
    s
  end

  def <=> (other)
    to_seconds <=> other.to_seconds
  end

  def revert
    if @hours > 23
      @hours -= 24
    end
  end

  def add_day
    @hours +=24
  end

  def add_day_to(other)
    if different_days?(other)
      if other.to_seconds > to_seconds
        add_day
      else
        other.add_day
      end
    end
  end

  def absolute_difference(other)
    (to_seconds - other.to_seconds).abs
  end

  def get_hours
    @hours
  end

  def different_days?(other)
    if @hours > 22 && other.get_hours < 2
      true
    elsif @hours < 2 && other.get_hours > 22
      true
    end
    false
  end
end