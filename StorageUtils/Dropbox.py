import dropbox
from dropbox.exceptions import AuthError
from requests import ConnectionError


class Dropbox:
	"""
	
	Wrapper for Dropbox access.
	Access token is short-lived. You may need to refresh it.
	
	"""
	
	
	class AuthError(Exception):
		pass
	
	
	def __init__(self, access_token:str) -> None:
		"""
		
		Raises :
			AuthError
			
		"""
		
		try:
			self.dbx = dropbox.Dropbox(access_token)
		except AuthError:
			raise Dropbox.AuthError
	
	
	def usage_stats(self) -> tuple[bool,tuple[int,int,float]]:
		"""
		
		Calcolate storage stats.
		
		Returns:
			tuple (
					connected,
					tuple( used storage, free storage, percentage used )
				)
		
		"""
		
		try:
			info = self.dbx.users_get_space_usage()
		except ConnectionError:
			return False, (-1, -1, 0.0)
		used = info.used
		alloc = info.allocation._value._allocated_value
		return True, (used, alloc, round( (used / alloc) * 100, 2))
	
	
	def list_folder(self, path:str='') -> tuple[bool,dict]:
		"""
		
		List folder content.
		
		Args:
			path : path to folder.
		
		Returns:
			tuple (
					connected,
					dict {folders:list, files:list}
				)
		
		"""
		
		try:
			out = {'folders':[], 'files':[]}
			for file in self.dbx.files_list_folder(path).entries:
				if isinstance(file, dropbox.files.FileMetadata):
					out['files'].append(file.name)
				else:
					out['folders'].append(file.name)
			return True, out
		except Exception as e:
			print(e)
			return False, {}


	def download_file(self, dropbox_path:str, local_path:str) -> bool:
		"""
		
		Download file from Dropbox.
		
		Args:
			dropbox_path : path to file in Dropbox.
			local_path : path to local file.
		
		Returns:
			True if file downloaded, False otherwise.
		
		"""
		
		try:
			_, result = self.dbx.files_download(dropbox_path)
			with open(local_path, 'wb') as f:
				f.write(result.content)
			return True
		except Exception as e:
			print(e)
			return False
	
	
	def upload_file(self, local_path:str, dropbox_path:str) -> bool:
		"""
		
		Upload file to Dropbox.
		
		Args:
			local_path : path to local file.
			dropbox_path : path to file in Dropbox.
		
		Returns:
			True if file uploaded, False otherwise.
		
		"""
		
		try:
			overwrite = dropbox.files.WriteMode('overwrite')
			with open(local_path, 'rb') as f:
				_ = self.dbx.files_upload(
					f.read(),dropbox_path, mode=overwrite)
			return True
		except Exception as e:
			print(e)
			return False
