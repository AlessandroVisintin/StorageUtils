from StorageUtils.SQLite import SQLite


PATH = 'out/StorageUtils/test.db'
CONFIG = 'config/StorageUtils/db_config.yaml'

# open database
db = SQLite(PATH, config=CONFIG)

# insert rows
params = [
	(1,'Mark','Twain',23),
	(2,'Eloise','Thompson',34),
	(3,'Madeleine','Water',27),
	(4,'Alex','White',45),
	(5,'Sam','Smith',18)
]
db.fetch(name='insert_test', params=params)

# count rows
print('Number of rows: ', db.size('test'))

# get rows
rows = [row for row in db.fetch(name='select_alltest')]

# custom query
query = 'SELECT * FROM test WHERE age < 30;'
rows = [row for row in db.fetch(query=query)]

# create index
db.add_index('test_idx', 'test', 'name', if_not_exists=True)

# print schema
print('Schema:')
db.schema()