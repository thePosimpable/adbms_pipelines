import configparser, sys, os, sqlalchemy as database
from pipeline.db_setup.load_staging.mysql_queries import export_queries
from pipeline.utils import create_directory, clear_destdir

def create_database(config, db):
	SQLALCHEMY_DATABASE_URI = "mysql://%s:%s@%s/%s" % (
		config.get('MYSQL', 'MYSQL_USER'), 
		config.get('MYSQL', 'MYSQL_PASSWORD'),
		config.get('MYSQL', 'MYSQL_SERVER'), 
		config.get('MYSQL', db)
	)

	print(f"Connecting to MySQL server at {SQLALCHEMY_DATABASE_URI}")

	engine = database.create_engine(SQLALCHEMY_DATABASE_URI, echo = False)

	return engine

def run_mysql_export(engine, db, destdir):
	sql_queries = export_queries[db]

	with engine.begin() as conn:
		for (index, query) in enumerate(sql_queries):
			print(f"Exporting MySQL query {index + 1} of {len(sql_queries)}")
			conn.execute(query)

def get_output_path(config, db):
	return config.get('MYSQL', 'DHDC_DESTDIR_PATH') if db == 'DHDC_DB' else config.get('MYSQL', 'EXCELHOME_DESTDIR_PATH')
	
def export_mysql_tables(db):
	config = configparser.ConfigParser()
	config.read('./pipeline/config/appconfigs.cfg')

	engine = create_database(config, db)

	destdir = get_output_path(config, db)
	create_directory(destdir)
	clear_destdir(destdir)

	run_mysql_export(engine, db, destdir)
	engine.dispose()
	print(f"MySQL connection closed. \n")