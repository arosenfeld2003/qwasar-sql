require 'json'

class MyRedisClass
  def initialize
    if File.file?("./my_dump.rdb")
      self.restore
    else
      @database = Hash.new
    end
  end

  def my_set(key, value)
    @database[key] = value
  end

  def my_get(key)
    return @database[key]
  end

  def my_mset(pairs)
    pairs.each {|pair| my_set(pair[0], pair[1])}
  end

  def my_mget(keys)
    keys_array = []
    keys.each do |key|
      keys_array.push(my_get(key))
    end
    return keys_array
  end

  # e.g. pairs = ['a', '3'] ==> returns nil for my_mget('a')
  def my_del(pairs)
    pairs.each {|pair| @database.delete(pair[0])}
  end

  def my_exists(key)
    @database[key] ? true : false
  end

  def my_rename(key, new_key)
    if @database[key] && !@database[new_key]
      @database[new_key] = @database[key]
      return true
    else
      return false
    end
  end

  def backup
    # delete previous contents of backup
    if File.file?("./my_dump.rdb")
      File.truncate("./my_dump.rdb", 0)
    end

    File.open("./my_dump.rdb", "w") do |file|
      file.write(@database.to_json)
    end
  end

  def restore
    json_from_file = File.read("./my_dump.rdb")
    @database = JSON.parse(json_from_file)
  end
end

# tests
# my_redis_instance = MyRedisClass.new
# my_redis_instance.my_mset(['a', 3], ['b', 8], [3, 'i'])
# my_redis_instance.backup

# puts my_redis_instance.my_exists('a')
# => true if backup file exists; false if no backup file.
