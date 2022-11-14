from json import dump
from csv import DictReader
from datetime import datetime
from re import match

def parse_date(date_time, date_time_regex):
  raw_dt = match(date_time_regex, date_time).groups()
  raw_dt = list(map(int, raw_dt))
  year = raw_dt.pop(2)
  raw_dt.insert(0, year)
  date_time = datetime(*raw_dt)
  
  return date_time

def parse_data_row(row, date_time_regex):
  date_time = parse_date(row['Date/Time'], date_time_regex)
  lat, lon = float(row['Lat']), float(row['Lon'])
  base = row['Base']
  
  parsed_row = {
    'Date/Time': date_time,
    'Lat': lat,
    'Lon': lon,
    'Base': base
  }
  
  return parsed_row

def load_data_rows(csv_data_path, date_time_regex):
  with open(csv_data_path, 'r') as csv_data_file:
    reader = DictReader(csv_data_file, delimiter=',')
    data_json = {}
    for row_idx, row in enumerate(reader):
      row = parse_data_row(row, date_time_regex)
      data_json[row_idx] = row
      
    return data_json

def load_data_json(data_json, json_data_path):
  with open(json_data_path, 'w') as json_data_file:
    dump(data_json, json_data_file, default=str)

if __name__ == '__main__':
  data_path = './../data/uber-raw-data-jul14{}'
  csv_data_path = data_path.format('.csv')
  json_data_path = data_path.format('.json')
  date_time_regex = r'(\d{1,2})/(\d{1,2})/(\d{4}) (\d{1,2}):(\d{1,2}):(\d{1,2})'
  
  load_data_json(
    load_data_rows(
      csv_data_path, date_time_regex
    ),
    json_data_path
  )
