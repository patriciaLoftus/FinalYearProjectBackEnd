class DataFormat
  require 'date'
  def initialize (f_name)
    @data = []
    @i = 0
    @j = 0
    @folder = f_name
  end
public
  def format_all_files
    Dir.foreach(@folder) do |item|
      next if !item.include?('csv')
      format(item)
      write_to_file
      @data = []
    end
  end
private
  def write_to_file
    File.open('TestFile.csv', 'a') { |f|
      puts "File is open #{@j}"
      @data.each do |line|
        str = ''
        line.each do |segment|
          str.concat(segment.to_s)
          str.concat(',')
        end
        str.chop!
        f.puts(str)
      end
    }
    @j+=1
  end

  def format(file)
    puts 'starting'
    this_file = ''
    this_file.concat(@folder)
    this_file.concat('/').concat(file)
    file = this_file
    File.open(file).each do |line|
      break if line.include?(',,,')
      next if  line.include?(',,')

      arr = line.split(',')
      arr_new = []
      date = arr.at(0).to_i/1000000
      str_date = DateTime.strptime(date.to_s,'%s')
      arr_new.push(str_date.to_time.to_s) #time stamp
      arr_new.push(arr.at(4)) #date
      arr_new.push(str_date.cwday.to_s) #day of week
      arr_new.push(arr.at(3)) #line id
      arr_new.push(arr.at(5))#journey id
      arr_new.push(arr.at(12))#vehicle id
      arr_new.push(arr.at(9))#lon id
      arr_new.push(arr.at(8))#lat id
      if arr.at(13) == ('null')
        arr_new.push(-1)
      else
        arr_new.push(arr.at(13))
      end
      arr_new.push(arr.at(14))#at stop

      @i +=1
      @data.push(arr_new)
    end
    puts Time.now
  end
end



