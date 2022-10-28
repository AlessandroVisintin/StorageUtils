from JSONWrap.utils import load

import sqlite3
from pathlib import Path
from typing import Iterable, Callable, Union


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
				config : path to configuration file (YAML/JSON).
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
			self.cfg = load(kwargs['config'])
			del kwargs['config']
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
		except (KeyError, TypeError):
			pass



	def __del__(self):
		"""
		
		Closing operations.
		
		"""
		
		print('Optimizing before closing..')
		with self.db as conn:
			conn.execute('PRAGMA optimize;')
		
		self.db.close()
	
	
	def _exe(self, **kwargs) -> sqlite3.Cursor:
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
			sqlite Cursor
		
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
			try:
				if isinstance(kwargs['params'], list):
					return conn.executemany(query, kwargs['params'])
				else:
					return conn.execute(query, kwargs['params'])
			except KeyError:
				return conn.execute(query)
	
	
	def schema(self) -> None:
		"""
		
		Print schema details of database.
		
		"""
		
		with self.db as conn:
			query = "SELECT name FROM sqlite_master WHERE type='table';"
			for row in conn.execute(query):
				print(row[0])
				cursor = conn.execute(f'SELECT * FROM {row[0]};')
				print([description[0] for description in cursor.description])
				print('\n')
	
	
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
	
	
	def add_function(self, name:str, params:int, reference:Callable) -> None:
		"""
		
		Add session function to perform queries.
		
		Args : 
			name : name of the function
			params : number of params of the function
			reference : reference to the function
		
		"""
		
		with self.db as conn:
			conn.create_function(name, params, reference)


	def add_index(self, name:str, table:str,
					cols:Union[str,Iterable], **kwargs) -> None:
		"""
		
		Add index to database.
		
		Args:
			name : name of the index.
			table : name of the table.
			cols : column(s) to index.
			**kwargs :
				unique : add UNIQUE keyword
				if_not_exists : add IF NOT EXISTS keyword
	
		"""
		
		return self._exe(
			
			query=('CREATE {unique} INDEX {if_not_exists} {name} '
				'ON {table}({cols});'),
			
			format={
				'unique' : 'UNIQUE' if 'unique' in kwargs and kwargs['unique'] else '',
				'if_not_exists' : 'IF NOT EXISTS' if 'if_not_exists' in kwargs and kwargs['if_not_exists'] else '',
				'name' : name,
				'table' : table,
				'cols' : cols if isinstance(cols,str) else ','.join(str(x) for x in cols)				
				}
			
			).fetchall()
	
	
	def drop(self, kind:str, name:str, **kwargs) -> None:
		"""

		Drop table.

		Args:
			kind : [view | table | index]
			name : name of the index.
			**kwargs :
				if_exists : add IF NOT EXISTS keyword

		"""
		
		return self._exe(
			
			query=('DROP {kind} {if_exists} {table};'),
			
			format={
				'kind' : kind.upper(),
				'if_exists' : 'IF EXISTS' if 'if_exists' in kwargs and kwargs['if_exists'] else '',
				'table' : name
				}
			
			).fetchall() 


	def yields(self, **kwargs) -> Iterable:
		"""
		
		Yield the results of a query on the database.
		
		Args :
			**kwargs : see _exe() for details.
		
		Returns :
			Yields result of query
				
		
		"""
		
		for row in self._exe(**kwargs):
			yield row
	
	
	def fetch(self, **kwargs) -> list[tuple]:
		"""
		
		Execute and fetch the results of a query on the database.
		
		Args :
			**kwargs : see _exe() for details.
		
		Returns :
			list of resulting tuples.
				
		
		"""
		
		return self._exe(**kwargs).fetchall()