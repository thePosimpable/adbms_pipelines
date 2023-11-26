import psycopg2, pandas as pd
from config import config

def frequent_orders_per_month(cur, conn):
	# years = [2018, 2019, 2020, 2021]
	years = [2021]
	months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

	masterlist = []

	for year in years:
		for month in months:
			query = """
				SELECT
					dim_customers.customerid,
					dim_customers.customerName,
					'%s' as year,
					'%s' as month,
					COUNT(fact_customer_orders.fact_customer_orders_id) as total_orders
					
				FROM fact_customer_orders
				JOIN dim_customers ON dim_customers.customerid = fact_customer_orders.customerid
				JOIN dim_customerdr_dates ON dim_customerdr_dates.drdate = fact_customer_orders.drdate

				WHERE 
					fact_customer_orders.dramount <> 0 AND
					dim_customerdr_dates.year = %s AND
					dim_customerdr_dates.month = %s

				GROUP BY dim_customers.customerid, dim_customers.customerName
				ORDER BY total_orders DESC
				LIMIT 5;
			""" % (year, month, year, month)

			# SELECT
			# 	dim_customers.customerid,
			# 	dim_customers.customerName,
			# 	'%s' as year,
			# 	'%s' as month,
			# 	COUNT(fact_customer_orders.fact_customer_orders_id) as total_orders
				
			# FROM fact_customer_orders
			# JOIN dim_customers ON dim_customers.customerid = fact_customer_orders.customerid
			# JOIN dim_customerdr_dates ON dim_customerdr_dates.drdate = fact_customer_orders.drdate

			# WHERE 
			# 	fact_customer_orders.dramount <> 0 AND
			# 	dim_customerdr_dates.year = %s AND
			# 	dim_customerdr_dates.month = %s

			# GROUP BY dim_customers.customerid, dim_customers.customerName
			# ORDER BY total_orders DESC
			# LIMIT 5;

			data = pd.read_sql_query(query, con = conn)
			masterlist.append(data)

	masterlist = pd.concat(masterlist)

	agg = masterlist.groupby(['customerid','customername']).count()
	agg = agg.rename(columns={'total_orders': 'months as top customer'})
	agg = agg.sort_values(by = ['months as top customer'], ascending = False)
	agg = agg.drop(columns = ["year", "month"])
	agg.to_csv(f"top_customers_byorder_each_month.csv"	, encoding='utf-8-sig')

	orders_df = masterlist[masterlist['customerid'].isin([1086, 1078, 1085, 52, 206, 898])]
	orders_df = orders_df.sort_values(by = ['customerid', 'customername', 'year', 'month' ], ascending = False)
	orders_df.to_csv(f"test.csv", encoding='utf-8-sig', index=False)	
	
def main(config, db):
	conn = psycopg2.connect(
		user = config.get('POSTGRES', 'USER'),
		password = config.get('POSTGRES', 'PASSWORD'),
		port = config.get('POSTGRES', 'PORT'),
		database = config.get('POSTGRES', db)
	)

	cur = conn.cursor()

	frequent_orders_per_month(cur, conn)
	
if __name__ == '__main__':
	db = 'dhdc_db'.upper()
	main(config = config, db = db)