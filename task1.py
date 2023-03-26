import os
import csv
from concurrent import futures

def read_file(file_path):
    # Read the data from the CSV file.
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        file_content = [row for row in reader]
    # Returns file content 
    return file_content

def get_files_data(input_dir):
    # files dir list
    files = [os.path.join(input_dir, file) for file in os.listdir(input_dir)]
    files_data = []
    
    # Read file in parallel
    with futures.ProcessPoolExecutor() as pool:
        for data in pool.map(read_file, files):
            files_data.extend(data)
        return files_data

def mapper(clicks, key):
    # Map data/cliks
    return list(map(lambda click: {
        'key': click[key],
        'value': 1
    }, clicks))
    
def partitioner(mapped_data):
    # Group the key-value pairs
    grouped_data = {}

    for data in mapped_data:
        key = data['key']
        value = data['value']
        if key in grouped_data:
            grouped_data[key].append(value)
        else:
            grouped_data[key] = [value]
     
    return grouped_data

def reducer(grouped_data, key):
    return sorted(
        [{ key: k, 'count': len(v)} for k, v in grouped_data.items()],
        key=lambda x: x[key]
    )

def write_to_csv_file(output_file, reduced_data, key):
    output_file = output_file + '.csv'
    # Write the outpoutput_fileut data to a CSV file.
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([key, 'count'])
        for row in reduced_data:
            writer.writerow([row[key], row['count']])

def map_reduce(input_dir, key, output_file):
    """
    input_dir: (stirng) input files directory
    key: (string) column name to count
    output_file: (string) output files name
    """
    # Get the base directory
    base_dir = os.path.dirname(os.path.abspath(__file__))

    input_dir = base_dir + '/' + input_dir
    output_file = base_dir + '/' + output_file

    files_data = get_files_data(input_dir)
    mapped_data = mapper(files_data, key)
    grouped_data = partitioner(mapped_data)
    reduced_data = reducer(grouped_data, key)
    write_to_csv_file(output_file, reduced_data, key)
    # return reduced_data

if __name__ == '__main__':

    map_reduce('data/clicks', 'click_target', 'data/clicks_per_day')