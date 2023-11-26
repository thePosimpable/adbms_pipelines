import psycopg2
from pipeline.db_setup.load_staging.postgres_queries import create_table_queries, drop_table_queries

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
		print(f"Running drop staging query {index + 1} of {len(drop_table_queries)}.")

		cur.execute(query)
		conn.commit()

	print()

def create_tables(cur, conn):
	for (index, query) in enumerate(create_table_queries):
		print(f"Running create staging query {index + 1} of {len(create_table_queries)}.")

		cur.execute(query)
		conn.commit()

	print()

def create_staging(config, db):
	print("Running create_staging.py")
	db = db.upper()

	cur, conn = create_database(config, db)
	
	drop_tables(cur, conn)
	create_tables(cur, conn)

	conn.close()
	print(f"Disconnected from {db} Postgres DB")
	print("End create_staging.py\n")