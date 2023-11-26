import sys, time, psycopg2, configparser, os, sqlalchemy as database
from pipeline.db_setup.load_staging import export_mysql_tables, CSV_MAPPING

CSV_PATH = {
	"DHDC_DB": "C:/Users/Public/adbms_dwh/dhdc",
	"EXCELHOME_DB": "C:/Users/Public/adbms_dwh/excelhome"
}

def load_to_postgres(config, db):
	print(f"Connecting to {db} Postgres DB.")

	conn = psycopg2.connect(
		user = config.get('POSTGRES', 'USER'),
		password = config.get('POSTGRES', 'PASSWORD'),
		port = config.get('POSTGRES', 'PORT'),
		database = config.get('POSTGRES', db)
	)

	cur = conn.cursor()

	print(f"Loading CSVs to {db} Postgres DB.")

	for file in os.listdir(CSV_PATH[db]):
		f = open(f'{CSV_PATH[db]}/{file}', 'r')

		SQL = CSV_MAPPING[file]["query"]
		STAGING_TABLE_NAME = CSV_MAPPING[file]["table"]
	
		cur.copy_expert(sql = SQL % STAGING_TABLE_NAME, file = f)
		print(f"Loaded {CSV_PATH[db]}/{file}.")

	conn.commit()
	conn.close()
	print(f"{db} Postgres DB connection closed.")
	print(f"Finished loading CSVs to {db} Postgres DB.")

def etl_staging(config, db):
	print("Running etl_staging.py")

	db = db.upper()
	export_mysql_tables(db)
	load_to_postgres(config, db)

	print("End etl_staging.py\n")