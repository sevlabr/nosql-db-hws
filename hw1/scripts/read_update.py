from pprint import pformat
from pymongo import MongoClient


class StatsCalculator:
    '''
    Performs basic statistical operations
    on a defined collection given attribute
    name.
    Available operations: max(), min(), avg(),
    median(), count().
    '''
    
    def __init__(self, collection):
        self.collection = collection
        
    def count_docs(self, attr_name, attr_val):
        # counts documents where attr_name equals attr_val
        return self.collection.count_documents({attr_name: attr_val})
    
    def calc_basic_stats(self, attr_name, method='max'):
        # return basic stats for attr_name
        # methods: 'max', 'min', 'avg'
        pipeline = [
            {'$group':
             {'_id': None,
              method: {f'${method}': f'${attr_name}'}}
            }
        ]
        aggr_res = self.collection.aggregate(pipeline)
        stat_res = list(aggr_res)[0][method]
    
        return stat_res
    
    def calc_full_basic_stats(self, attr_name):
        # return all basic stats simultaneously
        pipeline = [
            {'$group':
             {'_id': None,
              'max': {'$max': f'${attr_name}'},
              'min': {'$min': f'${attr_name}'},
              'avg': {'$avg': f'${attr_name}'},
             }
            }
        ]
        aggr_res = self.collection.aggregate(pipeline)
        stats_res = list(aggr_res)[0]
        max_val = stats_res['max']
        min_val = stats_res['min']
        avg_val = stats_res['avg']
    
        return max_val, min_val, avg_val
    
    def calc_median(self, attr_name):
        # return median value for attr_name
        count_docs = self.collection.count_documents({})
        cd_half = int(count_docs / 2 - 1)
        doc_med = self.collection.find({}).sort(attr_name, 1).skip(cd_half).limit(1)
        med_val = list(doc_med)[0][attr_name]
        
        return med_val


def output_basic_stats(out_file_name, collection, mode='w', wine_type="white"):
    with open(out_file_name, mode) as out_file:
        out_file.write(f'Basic statistics for {wine_type} wine:\n')
        out_file.write('MIN, MAX, AVG, MEDIAN\n')
    
        wine_calc = StatsCalculator(collection)
        attr_names = collection.find_one({}).keys()
        for attr_name in attr_names:
            if attr_name != '_id':
                out_file.write('\t' + attr_name + ': ')
                min_val, max_val, avg_val = wine_calc.calc_full_basic_stats(attr_name)
                median_val = wine_calc.calc_median(attr_name)
                vals = list(
                    map(
                        lambda val: str(round(val, 2)),
                        [min_val, max_val, avg_val, median_val]
                    )
                )
                stats_for_attr = ', '.join(vals)
                out_file.write(stats_for_attr + '\n')


def output_max_min_vals(out_file_name, collection, attr_name, mode='a'):
    wine_calc = StatsCalculator(collection)
    max_val_count = wine_calc.count_docs(
        attr_name,
        wine_calc.calc_basic_stats(attr_name, method='max')
    )
    min_val_count = wine_calc.count_docs(
        attr_name,
        wine_calc.calc_basic_stats(attr_name, method='min')
    )
    
    with open(out_file_name, mode) as out_file:
        vals = list(map(str, [max_val_count, min_val_count]))
        out_file.write(', '.join(vals) + '\n')


def add_line(out_file_name, line='\n', mode='a'):
    with open(out_file_name, mode) as out_file:
        out_file.write(line)


def some_read_queries(output_file_name, db):
    # performs .aggregate() and .find() operations and writes result to the file
    
    # write basic statistics
    output_basic_stats(output_file_name, db.white, mode='w', wine_type="white")
    add_line(output_file_name, line='\n', mode='a')
    output_basic_stats(output_file_name, db.red, mode='a', wine_type="red")
    add_line(output_file_name, line='\n', mode='a')
    
    # write number of wines where quality
    # is the best and the worst
    add_line(output_file_name, line='Max and min quality count for red wine: ', mode='a')
    output_max_min_vals(output_file_name, db.red, 'quality', mode='a')
    add_line(output_file_name, line='Max and min quality count for white wine: ', mode='a')
    output_max_min_vals(output_file_name, db.white, 'quality', mode='a')
    add_line(output_file_name, line='\n\n', mode='a')
    
    # some different .find() queries
    query_filter = {
        'quality': {'$lte': 6},
        'alcohol': {'$gte': 11},
        'residual sugar': {'$gt': 5.0}
    }
    cols_to_output = {
        'alcohol': 1,
        'quality': 1,
        'residual sugar': 1,
        'total sulfur dioxide': 1
    }
    res = list(
        db.red.find(
            query_filter, cols_to_output
        ).sort('total sulfur dioxide', -1).limit(2)
    )
    line = 'Red wines with quality <= 6 and alcohol >= 11'\
            + ' and residual sugar > 5.0, sorted by total\n'\
            + '  sulfur dioxide in descending order. Only'\
            + ' fisrt 2 are shown:\n\n'

    add_line(output_file_name, line=line, mode='a')
    add_line(output_file_name, line=pformat(res), mode='a')
    add_line(output_file_name, line='\n\n', mode='a')

    query_filter = {
        '$or': [
            {'alcohol': {'$lte': 7.5}},
            {'quality': {'$gte': 8}}
        ],
        'pH': {'$lte': 3.0},
        'fixed acidity': {'$in': [8.0, 7.5]}
    }
    cols_to_output = {
        'alcohol': 1,
        'quality': 1,
        'pH': 1,
        'fixed acidity': 1
    }
    res = list(db.white.find(query_filter, cols_to_output))
    line = 'White wines with alcohol <= 7.5 or quality >= 8'\
            + ' which have pH <= 3.0\n'\
            + '  and fixed acidity either 8.0 or 7.5. Full '\
            + 'query result is shown:\n\n'

    add_line(output_file_name, line=line, mode='a')
    add_line(output_file_name, line=pformat(res), mode='a')
    add_line(output_file_name, line='\n\n', mode='a')

    # find max density and max alcohol
    # for best quality wines
    pipeline = [
        {'$group':
         {'_id': '$quality',
          'max_dens': {'$max': '$density'},
          'max_alc': {'$max': '$alcohol'}
         }
        },
        {'$sort': {'_id': -1}}
    ]
    aggr_res = list(db.red.aggregate(pipeline))
    res_best_quality = aggr_res[0]
    best_quality = str(res_best_quality['_id'])
    max_dens = str(res_best_quality['max_dens'])
    max_alc = str(res_best_quality['max_alc'])
    line = 'Red wines with best quality of '\
            + best_quality + ' reach maximum '\
            + 'density ' + max_dens + ' or '\
            + 'maximum alcohol ' + max_alc + '\n\n\n'
    add_line(output_file_name, line=line, mode='a')


def some_update_queries(output_file_name, db):
    # performs some $inc and $set updates and reports to the output file

    # if quality >= 7 increase it by 1 for red wines
    db.red.update_many(
        {'quality': {'$gte': 7}},
        {'$inc': {'quality': 1}}
    )
    
    line = 'Increased quality by 1 for red wines where quality >= 7\n'
    add_line(output_file_name, line=line, mode='a')
    red_wine_calc = StatsCalculator(db.red)
    min_val, max_val, avg_val = red_wine_calc.calc_full_basic_stats('quality')
    median_val = red_wine_calc.calc_median('quality')
    vals = list(
        map(
            lambda val: str(round(val, 2)),
            [min_val, max_val, avg_val, median_val]
        )
    )
    line = 'Now stats for red wine quality are: '\
            + ', '.join(vals) + '\n\n'
    add_line(output_file_name, line=line, mode='a')
    
    db.white.update_many(
        {'quality': {'$gte': 8}, 'pH': {'$gt': 3}},
        {'$set': {'quality': -10, 'density': 30, 'pH': -100}}
    )
    
    line = 'Set quality -10, density 30 and pH -100 for white wines '\
            + 'where quality >= 8 and pH >= 3\n'
    add_line(output_file_name, line=line, mode='a')
    line = 'Now stats for white wine are:\n'
    add_line(output_file_name, line=line, mode='a')
    output_basic_stats(output_file_name, db.white, mode='a', wine_type="white")


if __name__ == '__main__':
    mongo_client = MongoClient('mongodb://rootu:rootp@localhost:27017')
    db = mongo_client.wine_quality
    output_file_name = 'read_update_logs.txt'
    
    some_read_queries(output_file_name, db)
    some_update_queries(output_file_name, db)

