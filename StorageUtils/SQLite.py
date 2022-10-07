from JSONWrap.utils import load

import sqlite3
from pathlib import Path
from typing import Iterable


class SQLite:
	"""
	
	Wrapper for managing SQLite databases.
	
	"""
	
	
	def __init__(self, config_file:str) -> None:
		"""
		
		Args:
			config_file : configuration file path (YAML/JSON).
				Syntax:
					details :
						name : str
						location : str
						same_thread : (true|false)
					create :
						table : query
						...
					insert :
						table : query
						...

		Raises:
			RuntimeError
 
		"""
		
		self.cfg = load(config_file)
		try:
			name = self.cfg['details']['name']
			location = self.cfg['details']['location']
			thread = self.cfg['details']['same_thread']
			Path(location).mkdir(parents=True,exist_ok=True)
			self.db = sqlite3.connect(
				f'{location}/{name}', check_same_thread=thread)
			with self.db as conn:
				for name,query in self.cfg['create'].items():
					conn.execute(query)
		except KeyError:
			raise RuntimeError('Missing mandatory key.')
		except sqlite3.OperationalError as e:
			raise RuntimeError(e)


	def __del__(self):
		"""
		
		Closing operations.
		
		"""
		
		self.conn.close()


	def insert(self, table, values:list[tuple]) -> None:
		"""
		
		Insert rows in database.
		
		Args:
			table : table where to insert.
			values : list of tuples with row values.

		"""
		
		query = self.cfg['insert'][table]
		with self.db as conn:
			conn.executemany(query, values)
				
	
	def select(self, query_name) -> Iterable[tuple]:
		"""
		
		Yields rows from database.
		
		Args:
			query_name : select query to execute.
				Must match with config_file provided.

		"""
		
		query = self.cfg['select'][query_name]
		with self.db as conn:
			for row in conn.execute(query):
				yield row
