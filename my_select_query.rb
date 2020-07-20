require 'csv'

class MySelectQuery
  def initialize(csv_content)
    data_arr  = CSV.parse(csv_content)
    column_titles = data_arr.shift
    if column_titles[0] == nil
      column_titles.shift
      data_arr.each {|player| player.shift}
    end
    @player_data = createDataSummary(column_titles, data_arr)
    return @player_data
  end

  def createDataSummary(column_titles, data)
    data_summary = []
    data.each do |player|
      data_summary.push({})
      player.each_index do |index|
        player_hash = data_summary[data_summary.length - 1]
        player_hash[:"#{column_titles[index]}"] = "#{player[index]}"
      end
    end
    return data_summary
  end

  def where(column_name, criteria)
    @query_results = []
    @player_data.each do |player|
      if player[:"#{column_name}"] == criteria
        @query_results.push(player.values.join(','))
      end
    end
    return @query_results
  end
end

# tests
# filtered_player_data = MySelectQuery.new(File.read("Seasons_stats.csv"))
# filtered_player_data = MySelectQuery.new(File.read("nba_player_data.csv"))
# filtered_player_data = MySelectQuery.new(File.read("nba_players.csv"))
# print filtered_player_data.where("Player", "Curly Armstrong")


