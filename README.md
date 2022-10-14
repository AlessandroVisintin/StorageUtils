# StorageUtils
> Helper functions to ease the storage of data.

StorageUtils contains a series of modules that simplify the storage of data.

## Installation
Clone the project inside your working directory.
You can install the package locally by running pip at the root level.
```sh
pip install /path/to/root/level
```

## Usage examples
Manage a SQLite database with a config file.
```py

from StorageUtils.SQLite import SQLite


# path to config file
CONFIG = 'config/StorageUtils/db_config.yaml'

# open database
db = SQLite(CONFIG)

# insert rows
values = [
	(1,'Mark','Twain',23),
	(2,'Eloise','Thompson',34),
	(3,'Madeleine','Water',27)
	]
db.insert('test', values)

# get rows
rows = [row for row in db.select('all_test')]

```
Access Dropbox.
```py

from StorageUtils.Dropbox import Dropbox


# create connection using ACCESS_TOKEN
TOKEN = 'config/StorageUtils/dropbox_token.txt'
with open(TOKEN, 'r') as f:
	dbx = Dropbox(f.read())

# get storage stats
print(dbx.usage_stats(),'\n')

# list folder
print( dbx.list_folder('/test') )

#upload file
LOCAL = 'config/StorageUtils/dropbox_upload.json'
REMOTE = '/test/dropbox_upload.json'
done = dbx.upload_file(LOCAL, REMOTE)
print('Uploaded successfully?', done, '\n')

#download file
LOCAL = 'config/StorageUtils/dropbox_download.json'
done = dbx.download_file(REMOTE, LOCAL)
print('Downloaded successfully?', done, '\n')

```

## Meta
Alessandro Visintin - alevise.public@gmail.com

Find me: [Twitter](https://twitter.com/analog_cs) [Medium](https://medium.com/@analog_cs)

Distributed under the MIT license. See ``LICENSE.txt``.
