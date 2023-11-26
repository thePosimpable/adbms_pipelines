import sys
from dotenv import load_dotenv
from pipeline.config import config
from pipeline.db_setup import dropdb, create_staging, create_dwh
from pipeline.etl import etl_staging, etl_dwh, export_dwh

DOTENV_PATH = ".env"
load_dotenv(DOTENV_PATH)


def main(db):
	print(f"RUNNING {db} PIPELINE EXECUTION.\n")

	PARAMS = {
		"config": config,
		"db": db
	}

	# dropdb(
	# 	config = PARAMS['config'],
	# 	path = os.environ.get("DHDC_PIPELINE_PATH"),
	# 	filename = os.environ.get("DHDC_PIPELINE_FILENAME"),
	# 	db = os.environ.get("DHDC_PIPELINE_DATABASE"),
	# )

	create_staging(
		config = PARAMS['config'],
		db = PARAMS['db']
	)

	etl_staging(
		config = PARAMS['config'],
		db = PARAMS['db']
	)

	create_dwh(
		config = PARAMS['config'],
		db = PARAMS['db']
	)

	etl_dwh(
		config = PARAMS['config'],
		db = PARAMS['db']
	)

	# export_dwh(
	# 	config = PARAMS['config'],
	# 	db = PARAMS['db']
	# )

	print(f"FINISHED {db} PIPELINE EXECUTION.\n")

if __name__ == "__main__":
	# try:
	# 	db = sys.argv[1].upper()
	# 	main(db = db)

	# except:
	# 	print("Pipeline requires a database parameter.")
	# 	quit()

	db = sys.argv[1].upper()
	main(db = db)