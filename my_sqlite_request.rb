require 'csv'

class MySqliteRequest

  def initialize
    @datafile
    @data_table_from
    @column_titles_from = []
    @data_table_join
    @data_table_update
    @select_query = []
    @where_query = []
    @join_query = []
    @sort_query = []
    @insert_query = false
    @insert_values = {}
    @update_values = {}
    @delete_request = false
    @result_set = []
    class << self
      attr_accessor :result_set
    end
  end

  def create_data_table(column_titles, data)
    data_table = []
    # assigning our own ID.
    # what if dataset already has a unique id column?
    id = 0
    data.each do |row|
      data_hash = {}
      id += 1
      data_hash[:id_serial] = "#{id}"
      row.each_index do |index|
        data_hash[:"#{column_titles[index]}"] = "#{row[index]}"
      end
      data_table.push(data_hash)
    end
    return data_table
  end

  def filter_result_set
    join_query_match = false
    matched = []
    joined_rows = []

    @data_table_from.each do |data_hash|

      next if @where_query.length > 0 && (data_hash[:"#{@where_query[0]}"] != @where_query[1])

      if @join_query.length > 0
        val = data_hash[:"#{@join_query[0]}"]
        @data_table_join.each do |data_hash_join|
          if data_hash_join[:"#{@join_query[1]}"] == val && !matched.include?(data_hash_join)
            matched.push(data_hash_join)
            join_query_match = true
          end
        end
      end

      next if @join_query.length > 0 && join_query_match == false

      result = {}

      @select_query.each do |column|
        result[:"#{column}"] = data_hash[:"#{column}"]
      end

      @result_set.push(result)
    end

    joined_rows = filter_matching_rows(matched)
    joined_rows.each {|hash| @result_set.push(hash)}

    if @sort_query.length > 0
      if @sort_query[0] == 'ASC'
        @result_set = @result_set.sort_by{ |hash| hash[:"#{@sort_query[1]}"] }
      end
      if @sort_query[0] == 'DESC'
        @result_set = @result_set.sort_by{ |hash| hash[:"#{@sort_query[1]}"] }.reverse
      end
    end
  end

  def filter_matching_rows(matched_rows)
    matched = []
    matched_rows.each do |row|
      matched_columns = {}
      @select_query.each do |column_name|
        if row[:"#{column_name}"]
          matched_columns[:"#{column_name}"] = row[:"#{column_name}"]
        end
      end
      matched.push(matched_columns)
    end
    return matched
  end

  def write_values_to_file
    # we match keys with column names.
    # if a key exists that isn't a column name, we throw an error.
    # if a column name exists that is not a key, we insert a ',' on that column.
    columns = @insert_values.keys
    columns.each do |column|
      if !@column_titles_from.include?(column)
        STDERR.puts "Insert columns do not match data table"
        return
      end
    end

    new_row = []
    @column_titles_from.each_index do |index|
      if !columns.include?(@column_titles_from[index])
        new_row[index] = ','
      else
        col = @column_titles_from[index].to_s
        new_row[index] = @insert_values["#{col}"]
      end
    end

    File.write(@datafile, "\n", mode: "a")
    File.write(@datafile, new_row.join(","), mode: "a")
  end

  def update_values_in_data_table
    key = (@update_values.keys[0]).to_s
    @data_table_from.each do |data_hash_from|
      @result_set.each_index do |index|
        data_hash_result = result_set[index]
        if data_hash_from[:"#{@where_query[0]}"] == data_hash_result[:"#{@where_query[0]}"]
          data_hash_from[:"#{key}"] = @update_values[:"#{key}"]
          result_set[index][:"#{key}"] = @update_values[:"#{key}"]
        end
      end
    end
  end

  def update_values_in_file
    new_rows = []
    @data_table_from.each do |data_hash|
      new_row = []
      values = data_hash.values
      new_rows.push(values)
    end

    # remove added id_serial column
    new_rows.each{|row| row.shift}

    # write new data to the file
    File.write(@datafile, @column_titles_from.join(","))
    File.write(@datafile, "\n", mode: "a")

    new_rows.each_index do |index|
      File.write(@datafile, new_rows[index].join(","), mode: "a")
      if (index + 1) < new_rows.length
        File.write(@datafile, "\n", mode: "a")
      end
    end
  end

  def delete_values_from_data_table
    @data_table_from.each_index do |index|
      # if no @where_query, delete every value from the table.
      if @where_query.length == 0
        @data_table_from.delete_at(index)
      else
        # delete rows matching where query from table.
        data_hash = @data_table_from[index]
        if data_hash[:"#{@where_query[0]}"] == @where_query[1]
          @data_table_from.delete_at(index)
        end
      end
    end
  end

  def from(filename)
    @datafile = filename
    return self
  end

  def select(column_name)
    if column_name.instance_of?(Array)
      @select_query = column_name
    else
      @select_query.push(column_name)
    end
    return self
  end

  def where(column_name, criteria)
    @where_query.push(column_name)
    @where_query.push(criteria)
    return self
  end

  def join(column_on_db_a, filename_db_b, column_on_db_b)
    data_arr_b = CSV.parse(File.read(filename_db_b))
    column_titles_b = data_arr_b.shift
    @data_table_join = create_data_table(column_titles_b, data_arr_b)
    p @data_table_join
    @join_query.push(column_on_db_a)
    @join_query.push(column_on_db_b)
    return self
  end

  def order(order, column_name)
    @sort_query.push(order)
    @sort_query.push(column_name)
    return self
  end

  def insert(table_name)
    @datafile = table_name
    @insert_query = true
    return self
  end

  def values(data)
    @insert_values = data
    return self
  end

  def update(table_name)
    @datafile = table_name
    return self
  end

  def set(data)
    @update_values = data
    return self
  end

  def delete(table_name)
    @datafile = table_name
    @delete_request = true
    return self
  end

  def run
    data_arr_from = CSV.parse(File.read(@datafile))
    @column_titles_from = data_arr_from.shift
    @data_table_from = create_data_table(@column_titles_from, data_arr_from)

    # default select all columns
    if @select_query.length == 0 || @select_query[0] == '*'
      @select_query = @column_titles_from
    end

    filter_result_set()

    if !@update_values.empty?
      update_values_in_data_table()
      update_values_in_file()
    end

    if @delete_request == true
      delete_values_from_data_table()
      update_values_in_file()
    end

    if @insert_query == true
      write_values_to_file()
    end

    return @result_set

  end

end

# tests
# request = MySqliteRequest.new
# request = request.from('test_curly.csv')
# request = request.select('*')
# request = request.select('name')
# request = request.where('weight', '180')
# request = request.join('weight', 'test_johnny.csv', 'weight')
# request = request.order('DESC', 'name')
# request.insert('test_johnny.csv')

# request.values({
#   :name => 'Alex Rosenfeld',
#   :year_start =>'2020',
#   :year_end => '2020',
#   :position => 'G',
#   :height => '6-0',
#   :weight => '150',
#   :birth_date => "December 20, 1981",
#   :college => 'Northwestern University'
# })

# request.update('test_johnny.csv')
# request = request.where('name', 'Johnny Bach')
# request.set({:year_end => '2020'})

# request = request.delete('test_johnny.csv')
# request = request.where('name', 'Alex Rosenfeld')

# print request.run