import psycopg2, configparser, sys
from pipeline.db_setup.load_dwh.postgres_queries import create_table_queries, drop_table_queries

def create_database(config, db):
	print(f"Connected to {db} Postgres DB")

	conn = psycopg2.connect(
		user = config.get('POSTGRES', 'USER'),
		password = config.get('POSTGRES', 'PASSWORD'),
		port = config.get('POSTGRES', 'PORT'),
		database = config.get('POSTGRES', db)
	)

	conn.set_session(autocommit = False)
	cur = conn.cursor()

	return cur, conn

def drop_tables(cur, conn):
	for (index, query) in enumerate(drop_table_queries):
		print(f"Running drop dwh query {index + 1} of {len(drop_table_queries)}.")

		cur.execute(query)
		conn.commit()

	print()

def create_tables(cur, conn):
	for (index, query) in enumerate(create_table_queries):
		print(f"Running create dwh query {index + 1} of {len(create_table_queries)}.")

		cur.execute(query)
		conn.commit()

	print()

def create_dwh(config, db):
	print("Running create_dwh.py")
	db = db.upper()

	cur, conn = create_database(config, db)
	
	drop_tables(cur, conn)
	create_tables(cur, conn)

	conn.close()
	print(f"Disconnected from {db} Postgres DB")
	print("End create_dwh.py\n")