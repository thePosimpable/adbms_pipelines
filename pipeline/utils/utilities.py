import os

def create_directory(destdir):
	if os.path.exists(destdir) == False:
		os.mkdir(destdir)

def clear_destdir(destdir):
	for file in os.listdir(destdir):
		os.remove(f"{destdir}/{file}")

def generate_postgres_drop_queries(tables):
	return list(map(lambda table: f"DROP TABLE IF EXISTS {table};", tables))