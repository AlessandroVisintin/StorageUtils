from JSONWrap.utils import load

import sqlite3
from pathlib import Path
from typing import Iterable
from typing import Any


class SQLite:
	"""
	
	Wrapper for managing SQLite databases.
	
	"""
	
	
	def __init__(self, db_path:str, **kwargs) -> None:
		"""
		
		Args:
			db_path : path to database. Creates if not existing.
			**kwargs :
				same_thread (bool) : database connection used only
					by a single thread. Defaults to True.
				config_file : path to configuration file (YAML/JSON).
					Syntax:
						create :
							- query
							...
						defaults :
							name : query
							...

		Raises:
			RuntimeError
 
		"""
		
		try:
			self.cfg = load(kwargs['config_file'])
			del kwargs['config_file']
		except KeyError:
			self.cfg = None
		
		Path(Path(db_path).parent).mkdir(parents=True,exist_ok=True)
		self.db = sqlite3.connect(db_path, **kwargs)
		
		try:
			with self.db as conn:
				conn.execute('PRAGMA foreign_keys = ON;')
			with self.db as conn:
				for query in self.cfg['create']:
					conn.execute(query)
		except sqlite3.OperationalError as e:
			raise RuntimeError(e)
		except KeyError:
			pass



	def __del__(self):
		"""
		
		Closing operations.
		
		"""
		
		print('Optimizing before closing..')
		with self.db as conn:
			conn.execute('PRAGMA optimize;')
		
		self.db.close()
	
	
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


	def execute(self, **kwargs) -> Iterable:
		"""
		
		Execute a query on the database.
		
		Args :
			**kwargs :
				name (str) : execute a default query from config_file.
					It has precedence on 'query'.
				query (str) : execute a custom query.
				format (dict) : format query string with dictionary.
				params (tuple or list[tuple]) : parameters for parametrized
					query.
		
		Returns :
			Yields result of query
				
		
		"""
		
		query = None
		if 'name' in kwargs:
			try:
				query = self.cfg['defaults'][kwargs['name']]
			except KeyError:
				raise RuntimeError(f'Unknwon default query {kwargs["name"]}')
		elif 'query' in kwargs:
			query = kwargs['query']
		else:
			raise RuntimeError('Execute requires a query')
		
		if 'format' in kwargs:
			query = query.format(**kwargs['format'])
		
		with self.db as conn:
			cur = None
			try:
				if isinstance(kwargs['params'], list):
					cur = conn.executemany(query, kwargs['params'])
				else:
					cur = conn.execute(query, kwargs['params'])
			except KeyError:
				cur = conn.execute(query)
			
			for row in cur:
				yield row
				
		
		
		
		 

# 	def insert(self, query_name, values:list[tuple]) -> None:
# 		"""
# 		
# 		Insert rows in database.
# 		
# 		Args:
# 			query_name : table where to insert.
# 				Must match with key inside 'insert' in the config_file.
# 			values : list of tuples with row values.

# 		"""
# 		
# 		query = self.cfg['insert'][query_name]
# 		with self.db as conn:
# 			conn.executemany(query, values)

# 	
# 	def select(self, query_name, **kwargs) -> Iterable[tuple]:
# 		"""
# 		
# 		Yields rows from database.
# 		
# 		Args:
# 			query_name : select query to execute.
# 				Must match with key inside 'select' in the config_file.
# 			**kwargs :
# 				parameters : list of parameters

# 		"""
# 		
# 		query = self.cfg['select'][query_name]
# 		with self.db as conn:
# 			for row in conn.execute(query, **kwargs):
# 				yield row
