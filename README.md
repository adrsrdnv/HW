# HW
Homework


<b>TASK 1</b></br>
file task1.py

- input_dir: (stirng) input files directory
- key: (string) column name to count
- output_file: (string) output file name

map_reduce(input_dir, key, output_file)

e.g. map_reduce('data/clicks', 'date', 'data/clicks_per_day')


<b>TASK 2</b></br>
file task2.py

- first_input_dir: (stirng) first files directory
- column_filter: (string) column name to filter
- second_input_dir: (stirng) second files directory
- output_file: (string) output file name

map_reduce_join(first_input_dir, column_filter, second_input_dir, output_file)

e.g. map_reduce_join('data/users', 'LT', 'data/clicks','data/filtered_clicks')
