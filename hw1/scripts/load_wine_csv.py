import csv
from pymongo import MongoClient


def str_value_to_num(
    document,
    int_attrs=['quality']
):
    '''
    transform ints and floats from string to numerical format
    '''
    formatted_document = {}
    for key, value in document.items():
        if key in int_attrs:
            formatted_document[key] = int(value)
        elif key != '_id':
            formatted_document[key] = float(value)
      
    return formatted_document


def load_wine_csv():
    '''
    loads csv files with wine quality data to MongoDB
    '''
    # connect to Mongo
    mongo_client = MongoClient('mongodb://rootu:rootp@localhost:27017')
    # create db
    db = mongo_client.wine_quality
    # collections to be created
    collections = {'red': db.red, 'white': db.white}
    # convert csv to jsons and load into collection using insert_many()
    for wine_type in collections.keys():
        with open(f'data/winequality-{wine_type}.csv') as wine_data_csv:
            reader = csv.DictReader(wine_data_csv, delimiter=';')
            wine_documents = []
            for row in reader:
                row = str_value_to_num(row)
                wine_documents.append(row)
      
            collections[wine_type].insert_many(wine_documents)


if __name__ == "__main__":
    load_wine_csv()

