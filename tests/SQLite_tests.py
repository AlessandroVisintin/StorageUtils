from StorageUtils.SQLite import SQLite


# path to config file
CONFIG = 'config/StorageUtils/db_config.yaml'

# open database
db = SQLite(CONFIG)

# print schema
db.schema()

# count rows
print('Number of rows: ', db.size('test'))

# insert rows
values = [
	(1,'Mark','Twain',23),
	(2,'Eloise','Thompson',34),
	(3,'Madeleine','Water',27)
	]
db.insert('test', values)

# get rows
rows = [row for row in db.select('all_test')]
