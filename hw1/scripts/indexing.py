import numpy as np

from pymongo import MongoClient, ASCENDING
from time import time


def test_one_index(db, attr_name, val_range, repeat_range):
    start = time()
    for _ in repeat_range:
        for val in val_range:
            db.white.find({attr_name: val})
            db.red.find({attr_name: val})
    test_time = time() - start
    return test_time


def test_two_indices(
    db,
    attr_name_1, attr_name_2,
    val_range_1, val_range_2,
    repeat_range
):
    start = time()
    for _ in repeat_range:
        for val_1 in val_range_1:
            for val_2 in val_range_2:
                db.white.find({attr_name_1: val_1, attr_name_2: val_2})
                db.red.find({attr_name_1: val_1, attr_name_2: val_2})
    test_time = time() - start
    return test_time


def perform_tests(db):
    print('db.red indices:', list(db.red.index_information()))
    print('db.white indices:', list(db.white.index_information()))
    q_time = test_one_index(
        db, 'quality', range(1, 10), range(100)
    )
    alc_time = test_one_index(
        db, 'alcohol', np.arange(6, 15, 0.1), range(100)
    )
    tsd_time = test_one_index(
        db, 'total sulfur dioxide', np.arange(6, 440, 0.1), range(100)
    )
    total_time = q_time + alc_time + tsd_time
    print(
        'Total test time for singe attribute query:',
        round(total_time, 2), 'seconds'
    )
    total_time = test_two_indices(
        db,
        'quality', 'alcohol',
        range(1, 10), np.arange(6, 15, 0.1),
        range(100)
    )
    print(
        'Total test time for double attribute query:',
        round(total_time, 2), 'seconds'
    )


def create_simple_indices(db):
    print('\nCreating simple indices...')
    db.red.create_index([('quality', ASCENDING)])
    db.white.create_index([('quality', ASCENDING)])
    db.red.create_index([('alcohol', ASCENDING)])
    db.white.create_index([('alcohol', ASCENDING)])
    db.red.create_index([('total sulfur dioxide', ASCENDING)])
    db.white.create_index([('total sulfur dioxide', ASCENDING)])
    print('Simple indices created!\n')


def drop_simple_indices(db):
    print('\nDropping simple indices...')
    db.red.drop_index('quality_1')
    db.white.drop_index('quality_1')
    db.red.drop_index('alcohol_1')
    db.white.drop_index('alcohol_1')
    db.red.drop_index('total sulfur dioxide_1')
    db.white.drop_index('total sulfur dioxide_1')
    print('Dropped simple indices!\n')


def create_compound_index(db):
    print('\nCreating compound index for quality and alcohol...')
    db.red.create_index([('quality', ASCENDING), ('alcohol', ASCENDING)])
    db.white.create_index([('quality', ASCENDING), ('alcohol', ASCENDING)])
    print('Compound index created!\n')


def drop_compound_index(db):
    print('\nDropping compound index...')
    db.red.drop_index('quality_1_alcohol_1')
    db.white.drop_index('quality_1_alcohol_1')
    print('Dropped compound index!')


def test_indices(db):
    # tests without indices
    perform_tests(db)
    # simple indices
    create_simple_indices(db)
    perform_tests(db)
    drop_simple_indices(db)
    # compound index
    create_compound_index(db)
    perform_tests(db)
    drop_compound_index(db)


if __name__ == '__main__':
    mongo_client = MongoClient('mongodb://rootu:rootp@localhost:27017')
    db = mongo_client.wine_quality
    test_indices(db)

