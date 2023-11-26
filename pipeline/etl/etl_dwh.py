import sys, time, psycopg2, configparser, os, sqlalchemy as database
from pipeline.db_setup.load_dwh.postgres_queries import insert_table_queries

def load_to_postgres(config, db):
	conn = psycopg2.connect(
		user = config.get('POSTGRES', 'USER'),
		password = config.get('POSTGRES', 'PASSWORD'),
		port = config.get('POSTGRES', 'PORT'),
		database = config.get('POSTGRES', db)
	)

	cur = conn.cursor()

	for (index, query) in enumerate(insert_table_queries):
		print(f"Running insert dwh query {index + 1} of {len(insert_table_queries)}.")

		cur.execute(query)
		conn.commit()
		
	conn.close()
	print(f"Disconnected from {db} Postgres DB")

def etl_dwh(config, db):
	db = db.upper()

	print("Running etl_dwh.py")

	load_to_postgres(config, db)

	print("End etl_dwh.py\n")