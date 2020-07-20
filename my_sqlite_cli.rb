require "readline"
require "./my_sqlite_request.rb"

class My_sqlite_cli
  def initialize
    @user_input
    @insert_keys = []
    @insert_hash = {}
    @query_array = ['SELECT', 'INSERT INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'FROM', 'WHERE', 'JOIN', 'ON']
  end

  def assign_command_values(query)
    command_values = []
    @user_input.each_index do |index|
      if (@user_input[index] == query)
        index += 1
        while !@query_array.include?(@user_input[index]) && @user_input[index]
          args = @user_input[index]
          command_values.push(args)
          index += 1
        end
      end
    end
    return command_values
  end

  def parse_equal_query(query)
    column_name = []
    criteria = []
    new_query = []

    i = 0
    # all words BEFORE '=' (e.g. column_name).
    while query[i] && (query[i] != '=')
      column_name.push(query[i])
      i += 1
    end
    # we only handle equal ON, SET and WHERE requests.
    if query[i] != '='
      STDERR.puts "Query can only accept '='"
      return
    end
    i += 1
    # all words AFTER '=' (e.g. criteria).
    while query[i]
      criteria.push(query[i])
      i += 1
    end

    new_query.push(column_name.join(' '))
    new_query.push(criteria.join(' '))
    return new_query
  end

  def parse_values
    # remove () and group values inside () together based on comma separators
    @user_input.each_index do |index|
      if @user_input[index][0] == '('
        combined_vals = []
        while @user_input[index][-1] != ')'
          combined_vals.push(@user_input.slice!(index))
        end
        combined_vals.push(@user_input[index])
        str_vals = combined_vals.join(',')
        # remove '(' and ')'
        str_vals.slice!(0)
        str_vals.slice!(-1)
        char_array = str_vals.split('')
        char_array.each_index do |index|
          if char_array[index] == ','
            # array els are separated by two commas
            if char_array[index + 1] == ','
              # remove double comma
              char_array.slice!(index)
            else
              # replace single comma with a space
              char_array[index] = ' '
            end
          end
        end
        combined_vals = char_array.join.split(',')
        @user_input[index] = combined_vals
      end
    end
  end

  def parse_args(user_args)
    @user_input = user_args.split()

    # remove ';'
    last_arg = @user_input[@user_input.length - 1]
    if last_arg[last_arg.length - 1] == ';'
      @user_input[@user_input.length - 1].slice!(-1)
    end

    parse_values()

    # SELECT|INSERT INTO|VALUES|UPDATE|SET|DELETE|FROM|WHERE|JOIN|ON
    @user_input.each do |query|
      case query
      when 'SELECT'
        select_query = assign_command_values('SELECT')
        @request = @request.select(*select_query)
      when 'INSERT'
        if @user_input[1] != 'INTO'
          STDERR.puts "INSERT should be followed by INTO"
        else
          @user_input.slice!(1)
          insert_query = assign_command_values('INSERT')
          @request = @request.insert(insert_query[0])
          keys = insert_query[1]
          # save columns to build hashes for the insert
          keys.each {|key| @insert_keys.push(key)}
        end
      when 'VALUES'
        values_query = assign_command_values('VALUES')
        vals = values_query[0]
        vals.each_index do |index|
          # build hash for insert
          @insert_hash["#{@insert_keys[index]}"] = vals[index]
        end
        @request = @request.values(@insert_hash)
      when 'UPDATE'
        update_query = assign_command_values('UPDATE')
        @request = @request.update(*update_query)
      when 'SET'
        # example usage:
        # UPDATE test_johnny.csv WHERE name = Johnny Bach SET year_end = 2020
        set_query = assign_command_values('SET')
        new_set_query = parse_equal_query(set_query)
        set_query_hash = {}
        set_query_hash[:"#{new_set_query[0]}"] = new_set_query[1]
        @request = @request.set(set_query_hash)
      when 'DELETE'
        delete_query = assign_command_values('DELETE')
        @request = @request.delete(*delete_query)
      when 'FROM'
        from_query = assign_command_values('FROM')
        @request = @request.from(*from_query)
      when 'WHERE'
        # (max 1)
        where_query = assign_command_values('WHERE')
        # WHERE query will contain two values:
        new_where_query = parse_equal_query(where_query)
        @request = @request.where(*new_where_query)
      when 'JOIN'
        # (max 1)
        join_query = assign_command_values('JOIN')
        @join_on = []
        @join_on.push(join_query[0])
      when 'ON'
        # example usage:
        # SELECT * FROM file1.csv JOIN (file2.csv) WHERE (column1 = column2)
        on_query = assign_command_values('ON')
        new_join_query = parse_equal_query(on_query)
        @join_on.unshift(new_join_query[0])
        @join_on.push(new_join_query[1])
        @request = @request.join(*@join_on)
      end
    end
  end

  def run
    puts 'MySQLite version 0.1 2020-05-19'
    user_args = Readline.readline("my_sqlite_cli> ", true)
    while user_args != 'quit'
      @request = MySqliteRequest.new
      parse_args(user_args)
      puts @request.run
      user_args = Readline.readline("my_sqlite_cli> ", true)
    end
  end
end

app = My_sqlite_cli.new
app.run
