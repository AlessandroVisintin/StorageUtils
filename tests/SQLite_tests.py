from StorageUtils.SQLite import SQLite


PATH = 'out/StorageUtils/test.db'
CONFIG = 'config/StorageUtils/db_config.yaml'

# open database
db = SQLite(PATH, config_file=CONFIG)

# print schema
print('Schema:')
db.schema()

# insert rows
params = [
	(1,'Mark','Twain',23),
	(2,'Eloise','Thompson',34),
	(3,'Madeleine','Water',27),
	(4,'Alex','White',45),
	(5,'Sam','Smith',18)
]
db.execute(name='insert_test', params=params)

# count rows
print('Number of rows: ', db.size('test'))

# get rows
rows = [row for row in db.execute(name='select_alltest')]

# custom query
query = 'SELECT * FROM test WHERE age < 30;'
rows = [row for row in db.execute(query=query)]
