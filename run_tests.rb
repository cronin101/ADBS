require 'benchmark'

TEST_DATASET_SIZE = ARGV[0]
puts "Running test with #{TEST_DATASET_SIZE} elements."

seedfile_creation = Benchmark.realtime { `java WBGen sort_data #{TEST_DATASET_SIZE} > sort_data` }
puts "Seedfile creation took #{seedfile_creation} S."

seeding = Benchmark.realtime { `java -jar dist/attica.jar < sort_data` }
puts "Seeding the database took #{seeding} S."

`echo 'select sort_data.four, sort_data.unique1 from sort_data order by sort_data.four, sort_data.unique1;\nexit;' > sort_script`

output = ""
execution = Benchmark.realtime { output = `java -jar dist/attica.jar < sort_script` }
puts "Sorting the dataset took #{execution} S."

#puts "Output was:\n #{output}"
