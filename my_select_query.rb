require 'csv'

class MySelectQuery
  def initialize(csv_content)
    data_arr  = CSV.parse(csv_content)
    column_titles = data_arr.shift
    if column_titles[0] == nil
      column_titles.shift
      data_arr.each {|entry| entry.shift}
    end
    @data = createDataSummary(column_titles, data_arr)
    return @data
  end

  def createDataSummary(column_titles, data)
    data_summary = []
    data.each do |entry|
      data_summary.push({})
      entry.each_index do |index|
        entry_hash = data_summary[data_summary.length - 1]
        entry_hash[:"#{column_titles[index]}"] = "#{entry[index]}"
      end
    end
    return data_summary
  end

  def where(column_name, criteria)
    @query_results = []
    @data.each do |entry|
      if entry[:"#{column_name}"] == criteria
        @query_results.push(entry.values.join(','))
      end
    end
    return @query_results
  end
end

# tests
# filtered_player_data = MySelectQuery.new(File.read("./test-data/Seasons_stats.csv"))
# print filtered_player_data.where("Player", "Curly Armstrong")

# filtered_player_data = MySelectQuery.new(File.read("./test-data/nba_player_data.csv"))
# print filtered_player_data.where("name", "Curly Armstrong")

# filtered_player_data = MySelectQuery.new(File.read("./test-data/nba_players.csv"))
# print filtered_player_data.where("name", "Curly Armstrong")


