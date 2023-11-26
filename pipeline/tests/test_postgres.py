import psycopg2, configparser, sys

def test_db(config, db):
	try:
		conn = psycopg2.connect(
			user = config.get('POSTGRES', 'USER'),
			password = config.get('POSTGRES', 'PASSWORD'),
			port = config.get('POSTGRES', 'PORT'),
			database = config.get('POSTGRES', db)
		)

		print(f"{config.get('POSTGRES', db)} Postgres instance is UP.")
		conn.close()

	except:
		print(f"Could not establish connection with {config.get('POSTGRES', db)} Postgres DB.")

def main(db):
	config = configparser.ConfigParser()
	config.read('../appconfigs.cfg')

	test_db(config, db)

if __name__ == "__main__":
	# python test_postgres.py DHDC_DB
	# python test_postgres.py EXCELHOME_DB

	db = sys.argv[1]

	main(db)