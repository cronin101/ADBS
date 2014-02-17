require 'benchmark'

TEST_DATASET_SIZE = ARGV[0]

PIPE_MODE = ARGV[1] == '-p'

puts "Running test with #{TEST_DATASET_SIZE} elements." unless PIPE_MODE

seedfile_creation = Benchmark.realtime { `java WBGen sort_data #{TEST_DATASET_SIZE} > sort_data` }
puts "Seedfile creation took #{seedfile_creation} S." unless PIPE_MODE

seeding = Benchmark.realtime { `java -jar dist/attica.jar < sort_data` }
puts "Seeding the database took #{seeding} S." unless PIPE_MODE

`echo 'select sort_data.four, sort_data.unique1 from sort_data order by sort_data.four, sort_data.unique1;\nexit;' > sort_script`

output = ""
execution = Benchmark.realtime { output = `java -jar dist/attica.jar < sort_script` }
puts "Sorting the dataset took #{execution} S." unless PIPE_MODE

records = output.lines[7..-2]
  .map { |line| /\[(?<fst>\d+), (?<snd>\d+)\]/.match(line) }
  .map { |match| [match[:fst].to_i, match[:snd].to_i] }

records.each_with_index do |o, i|
  next if i < 1

  previous = records[i - 1]

  raise "Must be sorted by second attribute (#{previous}, #{o})" unless o.last >= previous.last
  raise "Must be subsorted by first attribute (#{previous}, #{o}" if (o.last == previous.last) && o.first < previous.first
  puts "(#{previous}, #{o})" if o.last == previous.last
end
