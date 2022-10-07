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
LOCAL = 'AVLib/AVStore/local_data/dropbox_download.json'
done = dbx.download_file(REMOTE, LOCAL)
print('Downloaded successfully?', done, '\n')
