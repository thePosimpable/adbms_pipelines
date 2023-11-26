import psycopg2, pandas as pd
from pipeline.db_setup.load_dwh.postgres_queries import export_table_queries
from pipeline.utils import create_directory, clear_destdir

def run_dwh_export(config, db, destdir):
	conn = psycopg2.connect(
		user = config.get('POSTGRES', 'USER'),
		password = config.get('POSTGRES', 'PASSWORD'),
		port = config.get('POSTGRES', 'PORT'),
		database = config.get('POSTGRES', db)
	)

	cur = conn.cursor()

	for (index, query) in enumerate(export_table_queries):
		print(f"Exporting DWH table {index + 1} of {len(export_table_queries)}")

		key = list(query.keys())[0]
		df = pd.read_sql_query(query[key], con = conn)
		df.to_csv(f"{destdir}/{key}.csv", encoding='utf-8-sig', index = False)	

	conn.close()
	print("Connection closed.")

def get_output_path(config, db):
	return config.get('POSTGRES', 'DHDC_DWH_DESTDIR') if db == 'DHDC_DB' else config.get('POSTGRES', 'EXCELHOME_DWH_DESTDIR')
	
def export_dwh(config, db):
	db = db.upper()

	print("Running export_dwh.py")

	destdir = get_output_path(config, db)
	create_directory(destdir)
	clear_destdir(destdir)

	run_dwh_export(config, db, destdir)

	print("End export_dwh.py\n")