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
    
    # Read files in parallel
    with futures.ProcessPoolExecutor() as pool:
        for data in pool.map(read_file, files):
            files_data.extend(data)
        return files_data

def write_to_csv_file(output_file, reduced_data):
    # Write the output data to a CSV file.
    output_file = output_file + '.csv'
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['user_id', 'country'])
        for row in reduced_data:
            writer.writerow([row['user_id'], row['country']])

def map_reduce_join(first_input_dir, column_filter, second_input_dir, output_file):
    """
    first_input_dir: (stirng) first files directory
    column_filter: (string) column name to filter
    second_input_dir: (stirng) second files directory
    output_file: (string) output files name
    """
    # Get the base directory 
    base_dir = os.path.dirname(os.path.abspath(__file__))

    users = get_files_data(base_dir + '/' + first_input_dir)
    clicks = get_files_data(base_dir + '/' + second_input_dir)

    # Map data/users. Filters by country code. 
    mapped_users = list(map(lambda user: {
        'key': user['id'],
        'value': user | {'table': 'users'}
    }, filter(lambda user: user['country'] == column_filter, users)))

    # Map data/clicks
    mapped_clicks = list(map(lambda click: {
        'key': click['user_id'],
        'value': None
    }, clicks))

    # Reduce
    reduced_data = []
    for key in set([item['key'] for item in mapped_users + mapped_clicks]):
        user_country = next((item['value'] for item in mapped_users if item['key'] == key), None)
        clicks = [item for item in mapped_clicks if item['key'] == key]
        if user_country is not None:
            for click in clicks:
                reduced_data.append({'user_id': click['key'], 'country': user_country['country']})
    # Sort by user_id 
    reduced_data = sorted(reduced_data, key=lambda x: x['user_id'])
    # Write to output file
    write_to_csv_file(base_dir + '/' + output_file, reduced_data)

    return mapped_users

if __name__ == '__main__':
    
    map_reduce_join('data/users', 'LT', 'data/clicks','data/filtered_clicks')
