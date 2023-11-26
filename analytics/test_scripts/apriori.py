import psycopg2, pandas as pd
from dotenv import load_dotenv

DOTENV_PATH = ".env"
load_dotenv(DOTENV_PATH)

conn = psycopg2.connect(
	user = os.environ.get("POSTGRES_USER"),
	password = os.environ.get("POSTGRES_PASSWORD"),
	port = os.environ.get("POSTGRES_PORT"),
	database = os.environ.get("POSTGRES_DATABASE"),
)

cur = conn.cursor()

query = """
	SELECT 
		drid,
		string_agg(productid::text, ', ') as products,
		string_agg(packageid::text, ', ') as packages
	
	FROM fact_item_sales
	GROUP BY drid
	ORDER BY drid;
"""

def remap_prods(x):
	vals = x

	if x != None:
		vals = x.split(", ")
		vals = ", ".join(list(map(lambda val: "product_" + val, vals)))

	return vals

def remap_packs(x):
	vals = x

	if x != None:
		vals = x.split(", ")
		vals = ", ".join(list(map(lambda val: "package_" + val, vals)))

	return vals

def clean_itemstring(x):
	newstring = x

	newstring = newstring if newstring[0] != "," else newstring[1:]
	newstring = newstring if newstring[-2] != "," else newstring[:-2]

	return newstring

df = pd.read_sql_query(query, con = conn)
# print(df.head())
df["products"] = df["products"].apply(remap_prods).fillna('')
df["packages"] = df["packages"].apply(remap_packs).fillna('')
# df["items"] = df['products'].astype(str) + (", " if df["packages"] != "" else "") + df['packages'].astype(str)

df["items"] = [', '.join(i) for i in zip(df['products'], df['packages'])]
df["items"] = df["items"].apply(clean_itemstring)

print(df.head(250))
filename = "apriori"
df.to_csv(f"C:/Users/Public/adbms_dwh/test_csvs/{filename}.csv", encoding='utf-8-sig', index = False)