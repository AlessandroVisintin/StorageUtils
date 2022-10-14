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
			# configuring
			with self.db as conn:
				conn.execute('PRAGMA foreign_keys = ON;')
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
		
		print('Optimizing before closing..')
		with self.db as conn:
			conn.execute('PRAGMA optimize;')
		
		self.db.close()
	
	
	def add_query(self, group:str, key:str, query:str) -> None:
		"""
		
		Add query to the config file.
		
		Args:
			group : group where to add the query
			key : name of the query
			query : query to add
		
		"""
		
		self.cfg[group][key] = query
	
	
	def schema(self) -> None:
		"""
		
		Print schema details of database.
		
		"""
		
		with self.db as conn:
			query = "SELECT name FROM sqlite_master WHERE type='table';"
			for row in conn.execute(query):
				query = f"SELECT sql FROM sqlite_schema WHERE name='{row[0]}';"
				print(conn.execute(query).fetchall()[0][0], '\n')
	
	
	def size(self, table_name:str) -> int:
		"""
		
		Count the number of rows in a table.
		
		Args : 
			table_name : name of the table.
		
		Returns :
			numbers of rows in the table.
		
		"""
		
		with self.db as conn:
			query = f'SELECT COUNT(*) AS count_{table_name} FROM {table_name};'
			count = conn.execute(query).fetchall()[0][0]
		return count
			
				

	def insert(self, query_name, values:list[tuple]) -> None:
		"""
		
		Insert rows in database.
		
		Args:
			query_name : table where to insert.
				Must match with key inside 'insert' in the config_file.
			values : list of tuples with row values.

		"""
		
		query = self.cfg['insert'][query_name]
		with self.db as conn:
			conn.executemany(query, values)

	
	def select(self, query_name, **kwargs) -> Iterable[tuple]:
		"""
		
		Yields rows from database.
		
		Args:
			query_name : select query to execute.
				Must match with key inside 'select' in the config_file.
			**kwargs :
				parameters : list of parameters

		"""
		
		query = self.cfg['select'][query_name]
		with self.db as conn:
			for row in conn.execute(query, **kwargs):
				yield row
