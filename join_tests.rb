require 'benchmark'

TEST_DATASET_1_SIZE = ARGV[0]
TEST_DATASET_2_SIZE = ARGV[1]

PIPE_MODE = ARGV[1] == '-p'

puts "Running test with #{TEST_DATASET_1_SIZE} and #{TEST_DATASET_2_SIZE} elements." unless PIPE_MODE

seedfile_creation =  Benchmark.realtime { `java WBGen join_data1 #{TEST_DATASET_1_SIZE} > join_data1` }
seedfile_creation += Benchmark.realtime { `java WBGen join_data2 #{TEST_DATASET_2_SIZE} > join_data2` }
puts "Seedfile creation took #{seedfile_creation} S." unless PIPE_MODE

seeding =  Benchmark.realtime { `java -jar dist/attica.jar < join_data1` }
seeding += Benchmark.realtime { `java -jar dist/attica.jar < join_data2` }
puts "Seeding the database took #{seeding} S." unless PIPE_MODE

`echo 'select join_data1.two, join_data2.two from join_data1, join_data2 where join_data1.unique1=join_data2.unique_1;\nexit;' > join_script`

sql = <<SQL
select join_data1.two from join_data1;
select join_data2.four from join_data2;
select join_data1.two, join_data2.four from join_data1, join_data2 where join_data1.two = join_data2.four;
exit;
SQL

`echo '#{sql}' > join_script`

output = ""
execution = Benchmark.realtime { output = `java -jar dist/attica.jar < join_script` }
puts "Sorting the dataset took #{execution} S." unless PIPE_MODE

puts output
=begin
records = output.lines[7..-2]
  .map { |line| /\[(?<fst>\d+), (?<snd>\d+)\]/.match(line) }
  .map { |match| [match[:fst].to_i, match[:snd].to_i] }

records.each_with_index do |o, i|
  next if i < 1

  previous = records[i - 1]

  raise "Must be sorted by second attribute (#{previous}, #{o})" unless o.last >= previous.last
  raise "Must be subsorted by first attribute (#{previous}, #{o}" if (o.last == previous.last) && o.first < previous.first
  #puts "(#{previous}, #{o})" if o.last == previous.last
end
=end
