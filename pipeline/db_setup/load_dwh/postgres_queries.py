from pipeline.utils import generate_postgres_drop_queries

class drop_dwh_queries:
	dwh_tables = [
		"fact_customer_sales",
		"fact_customer_orders",
		"fact_item_sales",
		"fact_dr_payments",
		"dim_cash_payments",
		"dim_check_payments",
		"dim_customerdr_dates",
		"dim_customerdrs",
		"dim_customers",
		"dim_areas",
		"dim_categories",
		"dim_products",
		"dim_packages"
	]

	drop_table_queries = generate_postgres_drop_queries(dwh_tables)

class create_dwh_queries:
	dim_areas_create = """
		CREATE TABLE IF NOT EXISTS dim_areas (
			dim_areas_id SERIAL PRIMARY KEY,
			areaid INT UNIQUE,
			rep TEXT,
			location TEXT,
			days_allowance INT
		);
	"""

	dim_customers_create = """
		CREATE TABLE IF NOT EXISTS dim_customers (
			dim_customers_id SERIAL PRIMARY KEY,
			customerid INT UNIQUE,
			customerName TEXT,
			customerAddress TEXT,
			customerContactNumber TEXT,
			areaid INT REFERENCES dim_areas (areaid)
		);
	"""

	dim_customerdrs_create = """
		CREATE TABLE IF NOT EXISTS dim_customerdrs (
			dim_customerdrs_id SERIAL PRIMARY KEY,
			drid INT UNIQUE,
			drno TEXT,
			drdate DATE,
			dramount DECIMAL(10,2),
			drbalance DECIMAL(10,2),
			drstatus TEXT,
			drterms TEXT,
			drterms_numeric INT NOT NULL,
			customerid INT REFERENCES dim_customers (customerid),
			areaid INT REFERENCES dim_areas (areaid)
		);
	"""

	dim_customerdr_dates_create = """
		CREATE TABLE IF NOT EXISTS dim_customerdr_dates (
			drdate DATE PRIMARY KEY,
			year INT,
			month INT,
			day INT,
			week INT,
			dow INT,
			quarter INT
		);
	"""

	dim_categories_create = """
		CREATE TABLE IF NOT EXISTS dim_categories (
			dim_categories_id SERIAL PRIMARY KEY,
			categoryid INT UNIQUE,
			categoryName TEXT
		);
	"""

	dim_products_create = """
		CREATE TABLE IF NOT EXISTS dim_products (
			dim_products_id SERIAL PRIMARY KEY,
			productid INT UNIQUE,
			unit TEXT, 
			productName TEXT,
			productSize TEXT,
			productSellingPrice DECIMAL(10, 2),
			productBuyingPrice DECIMAL(10, 2),
			productDiscount1 DECIMAL(10, 2),
			productDiscount2 DECIMAL(10, 2),
			productCode TEXT
		);
	"""

	dim_packages_create = """
		CREATE TABLE IF NOT EXISTS dim_packages (
			dim_packages_id SERIAL PRIMARY KEY,
			packageid INT UNIQUE,
			packageName TEXT,
			packageAmount DECIMAL(10, 2),
			packageSellingPrice DECIMAL(10, 2),
			packageBuyingPrice DECIMAL(10, 2),
			packageCode TEXT
		);
	"""

	fact_customer_sales_create = """
		CREATE TABLE IF NOT EXISTS fact_customer_sales (
			fact_customer_sales_id SERIAL PRIMARY KEY,
			drid INT,
			drno TEXT NOT NULL,
			customerid INT,
			areaid INT,
			drdate DATE NOT NULL,
			dramount DECIMAL(10,2) NOT NULL,
			drbalance DECIMAL(10,2) NOT NULL,
			drterms TEXT NOT NULL,
			drterms_numeric INT NOT NULL,
			drstatus TEXT NOT NULL,
			no_of_days INT NOT NULL,
			payment_window INT NOT NULL
		);
	"""

	fact_customer_orders_create = """
		CREATE TABLE IF NOT EXISTS fact_customer_orders (
			fact_customer_orders_id SERIAL PRIMARY KEY,
			drid INT,
			drno TEXT NOT NULL,
			customerid INT,
			areaid INT,
			drdate DATE NOT NULL,
			dramount DECIMAL(10,2) NOT NULL,
			drbalance DECIMAL(10,2) NOT NULL,
			drterms TEXT NOT NULL,
			drterms_numeric INT NOT NULL,
			drstatus TEXT NOT NULL,
			no_of_days INT NOT NULL,
			payment_window INT NOT NULL,
			created_at TIMESTAMP NOT NULL
		);
	"""

	fact_item_sales_create = """
		CREATE TABLE IF NOT EXISTS fact_item_sales (
			fact_item_sales_id SERIAL PRIMARY KEY,
			drid INT NOT NULL,
			drdate DATE NOT NULL,
			customerid INT NOT NULL,
			productid INT,
			unit TEXT,
			packageid INT,
			type TEXT,
			quantity INT NOT NULL,
			itemcost DECIMAL(10, 2) NOT NULL,
			discount1 DECIMAL(10, 2) NOT NULL,
			discount2 DECIMAL(10, 2) NOT NULL,
			itemcost2 DECIMAL(10, 2) NOT NULL,
			total_amount DECIMAL(10, 2) NOT NULL
		);
	"""

	dim_cash_payments_create = """
		CREATE TABLE IF NOT EXISTS dim_cash_payments (
			dim_cash_payments_id SERIAL PRIMARY KEY,
			paymentId INT UNIQUE,
			customerid INT REFERENCES dim_customers (customerid),
			amount DECIMAL(10,2),
			cash_date DATE,
			created_at TIMESTAMP
		);
	"""

	dim_check_payments_create = """
		CREATE TABLE IF NOT EXISTS dim_check_payments (
			dim_check_payments_id SERIAL PRIMARY KEY,
			paymentId INT UNIQUE,
			customerid INT REFERENCES dim_customers (customerid),
			amount DECIMAL(10,2),
			check_date DATE,
			bank TEXT,
			check_no TEXT,
			returned BOOLEAN,
			return_reason1 TEXT,
			return_reason2 TEXT,
			deposited BOOLEAN,
			created_at TIMESTAMP
		);
	"""

	dim_payment_dates_create = """
		CREATE TABLE IF NOT EXISTS dim_payment_dates (
			paymentDate DATE PRIMARY KEY,
			year INT,
			month INT,
			day INT,
			week INT,
			dow INT,
			quarter INT
		);
	"""

	fact_dr_payments_create = """
		CREATE TABLE IF NOT EXISTS fact_dr_payments (
			fact_dr_payments_id SERIAL PRIMARY KEY,
			paymentId INT,
			drid INT,
			customerid INT,
			payment_date DATE,
			payment_type TEXT,
			drdate DATE,
			drterms_numeric INT,
			areadaysallowance INT,
			payment_window INT,
			amount_paid DECIMAL(10, 2),
			created_at TIMESTAMP
		);
	"""

	create_table_queries = [
		dim_areas_create, 
		dim_customers_create, 
		dim_customerdrs_create, 
		dim_customerdr_dates_create, 
		fact_customer_sales_create, 
		fact_customer_orders_create,
		dim_categories_create,
		dim_products_create,
		dim_packages_create,
		fact_item_sales_create,
		dim_cash_payments_create,
		dim_check_payments_create,
		fact_dr_payments_create,
		dim_payment_dates_create
	]

class insert_dwh_queries:
	dim_areas_insert = ("""
		INSERT INTO dim_areas (
			areaid,
			rep,
			location,
			days_allowance
		)
		
		(
			SELECT
				CAST(areaid AS INT),
				arearepname,
				arealocation,
				CAST(areadaysallowance AS INT)

			FROM staging_areas
		);
	""")

	dim_customers_insert = ("""
		INSERT INTO dim_customers (
			customerid,
			customerName,
			customerAddress,
			customerContactNumber,
			areaid
		)
		
		(
			SELECT
				CAST(customerid AS INT),
				customerName,
				customerAddress,
				customerContactNumber,
				CAST(areaid AS INT)

			FROM staging_customers
		);
	""")

	dim_customerdrs_insert = ("""
		INSERT INTO dim_customerdrs (
			drid,
			drno,
			drdate,
			dramount,
			drbalance,
			drstatus,
			drterms,
			drterms_numeric,
			customerid,
			areaid
		)
		
		(
			SELECT
				CAST(drid AS INT),
				drno,
				CAST(drdate AS DATE),
				CAST(dramount AS DECIMAL(10, 2)),
				CAST(drbalance AS DECIMAL(10, 2)),
				drstatus,
				drterms,

				CAST(
					CASE
						WHEN staging_customerdrs.drterms = 'COD' OR staging_customerdrs.drterms = '' THEN 0
						WHEN staging_customerdrs.drterms LIKE '%15 Days%' THEN 15
						WHEN staging_customerdrs.drterms LIKE '%30 Days%' THEN 30
						WHEN staging_customerdrs.drterms LIKE '%45 Days%' THEN 45
						WHEN staging_customerdrs.drterms LIKE '%60 Days%' THEN 60
						WHEN staging_customerdrs.drterms LIKE '%75 Days%' THEN 75
						WHEN staging_customerdrs.drterms LIKE '%90 Days%' THEN 90
						WHEN staging_customerdrs.drterms LIKE '%120 Days%' THEN 120
					END 
				AS INT) AS drterms_numeric,

				CAST(customerid AS INT),
				CAST(areaid AS INT)

			FROM staging_customerdrs
		);
	""")

	dim_customerdr_dates_insert = ("""
		INSERT INTO dim_customerdr_dates (
			drdate,
			year,
			month,
			day,
			week,
			dow,
			quarter
		)
		
		(
			SELECT DISTINCT
				drdate,
				EXTRACT(YEAR FROM drdate),
				EXTRACT(MONTH FROM drdate),
				EXTRACT(DAY FROM drdate),
				EXTRACT(WEEK FROM drdate),
				EXTRACT(DOW FROM drdate),
				EXTRACT(QUARTER FROM drdate)

			FROM fact_customer_sales
		);
	""")

	dim_categories_insert = ("""
		INSERT INTO dim_categories (
			categoryid,
			categoryName
		)
		
		(
			SELECT
				CAST(staging_categories.categoryid AS INT),
				staging_categories.categoryname
				
			FROM staging_categories
		);
	""")

	dim_products_insert = ("""
		INSERT INTO dim_products (
			productid,
			unit,
			productName,
			productSize,
			productSellingPrice,
			productBuyingPrice,
			productDiscount1,
			productDiscount2,
			productCode
		)
		
		(
			SELECT
				CAST(staging_products.productid AS INT),
				staging_uoms.unitName,
				staging_products.productName,
				staging_products.productSize,
				CAST(staging_products.productSellingPrice AS DECIMAL(10,2)),
				CAST(staging_products.productBuyingPrice AS DECIMAL(10,2)),
				CAST(staging_products.productDiscount1 AS DECIMAL(10,2)),
				CAST(staging_products.productDiscount2 AS DECIMAL(10,2)),
				staging_products.productCode
				
			FROM staging_products
			JOIN staging_uoms ON staging_uoms.unitid = staging_products.unitId
		);
	""")

	dim_packages_insert = ("""
		INSERT INTO dim_packages (
			packageid,
			packageName,
			packageAmount,
			packageSellingPrice,
			packageBuyingPrice,
			packageCode
		)
		
		(
			SELECT
				CAST(packageid AS INT),
				packageName,
				CAST(packageAmount AS DECIMAL(10,2)),
				CAST(packageSellingPrice AS DECIMAL(10,2)),
				CAST(packageBuyingPrice AS DECIMAL(10,2)),
				packageCode
				
			FROM staging_packages
		);
	""")

	fact_customer_sales_insert = ("""
		INSERT INTO fact_customer_sales 
		(
			drid,
			drno,
			customerid,
			areaid,
			drdate,
			dramount,
			drbalance,
			drterms,
			drterms_numeric,
			drstatus,
			no_of_days,
			payment_window
		)

		(
			SELECT
				CAST(staging_customerdrs.drid AS INT),
				staging_customerdrs.drno,
				CAST(staging_customerdrs.customerid AS INT),
				CAST(staging_customerdrs.areaid AS INT),
				CAST(staging_customerdrs.drdate AS DATE),
				CAST(staging_customerdrs.dramount AS DECIMAL(10,2)),
				CAST(staging_customerdrs.drbalance AS DECIMAL(10,2)),

				CASE
					WHEN staging_customerdrs.drterms IS NULL THEN 'COD'
					ELSE staging_customerdrs.drterms
				END as drterms,
				
				CAST(
					CASE
						WHEN staging_customerdrs.drterms = 'COD' OR staging_customerdrs.drterms = '' THEN 0
						WHEN staging_customerdrs.drterms LIKE '%15 Days%' THEN 15
						WHEN staging_customerdrs.drterms LIKE '%30 Days%' THEN 30
						WHEN staging_customerdrs.drterms LIKE '%45 Days%' THEN 45
						WHEN staging_customerdrs.drterms LIKE '%60 Days%' THEN 60
						WHEN staging_customerdrs.drterms LIKE '%75 Days%' THEN 75
						WHEN staging_customerdrs.drterms LIKE '%90 Days%' THEN 90
						WHEN staging_customerdrs.drterms LIKE '%120 Days%' THEN 120
					END 
				AS INT) AS drterms_numeric,
					
				staging_customerdrs.drstatus,

				(CURRENT_DATE::date - staging_customerdrs.drdate::date) as noOfDays,

				(
					CASE
						WHEN staging_customerdrs.drterms = 'COD' OR staging_customerdrs.drterms = '' THEN 0
						WHEN staging_customerdrs.drterms LIKE '%15 Days%' THEN 15
						WHEN staging_customerdrs.drterms LIKE '%30 Days%' THEN 30
						WHEN staging_customerdrs.drterms LIKE '%45 Days%' THEN 45
						WHEN staging_customerdrs.drterms LIKE '%60 Days%' THEN 60
						WHEN staging_customerdrs.drterms LIKE '%75 Days%' THEN 75
						WHEN staging_customerdrs.drterms LIKE '%90 Days%' THEN 90
						WHEN staging_customerdrs.drterms LIKE '%120 Days%' THEN 120
					END
				) + dim_areas.days_allowance as payment_window
				
			FROM staging_customerdrs
			JOIN dim_areas ON dim_areas.areaid = CAST(staging_customerdrs.areaid AS INT)
			WHERE drstatus = 'Fully Paid'
		);
	""")

	fact_customer_orders_insert = ("""
		INSERT INTO fact_customer_orders
		(
			drid,
			drno,
			customerid,
			areaid,
			drdate,
			dramount,
			drbalance,
			drterms,
			drterms_numeric,
			drstatus,
			no_of_days,
			payment_window,
			created_at
		)

		(
			SELECT
				CAST(staging_customerdrs.drid AS INT),
				staging_customerdrs.drno,
				CAST(staging_customerdrs.customerid AS INT),
				CAST(staging_customerdrs.areaid AS INT),
				CAST(staging_customerdrs.drdate AS DATE),
				CAST(staging_customerdrs.dramount AS DECIMAL(10,2)),
				CAST(staging_customerdrs.drbalance AS DECIMAL(10,2)),

				CASE
					WHEN staging_customerdrs.drterms IS NULL THEN 'COD'
					ELSE staging_customerdrs.drterms
				END as drterms,
				
				CAST(
					CASE
						WHEN staging_customerdrs.drterms = 'COD' OR staging_customerdrs.drterms = '' THEN 0
						WHEN staging_customerdrs.drterms LIKE '%15 Days%' THEN 15
						WHEN staging_customerdrs.drterms LIKE '%30 Days%' THEN 30
						WHEN staging_customerdrs.drterms LIKE '%45 Days%' THEN 45
						WHEN staging_customerdrs.drterms LIKE '%60 Days%' THEN 60
						WHEN staging_customerdrs.drterms LIKE '%75 Days%' THEN 75
						WHEN staging_customerdrs.drterms LIKE '%90 Days%' THEN 90
						WHEN staging_customerdrs.drterms LIKE '%120 Days%' THEN 120
					END 
				AS INT) AS drterms_numeric,
					
				staging_customerdrs.drstatus,

				(CURRENT_DATE::date - staging_customerdrs.drdate::date) as noOfDays,

				(
					CASE
						WHEN staging_customerdrs.drterms = 'COD' OR staging_customerdrs.drterms = '' THEN 0
						WHEN staging_customerdrs.drterms LIKE '%15 Days%' THEN 15
						WHEN staging_customerdrs.drterms LIKE '%30 Days%' THEN 30
						WHEN staging_customerdrs.drterms LIKE '%45 Days%' THEN 45
						WHEN staging_customerdrs.drterms LIKE '%60 Days%' THEN 60
						WHEN staging_customerdrs.drterms LIKE '%75 Days%' THEN 75
						WHEN staging_customerdrs.drterms LIKE '%90 Days%' THEN 90
						WHEN staging_customerdrs.drterms LIKE '%120 Days%' THEN 120
					END
				) + dim_areas.days_allowance as payment_window,
				CAST(staging_customerdrs.created_at AS TIMESTAMP)
				
			FROM staging_customerdrs
			JOIN dim_areas ON dim_areas.areaid = CAST(staging_customerdrs.areaid AS INT)
		);
	""")

	fact_item_sales_insert = ("""
		INSERT INTO fact_item_sales
		(
			drid,
			drdate,
			customerid,
			productid,
			unit,
			packageid,
			type,
			quantity,
			itemcost,
			discount1,
			discount2,
			itemcost2,
			total_amount
		)

		(
			SELECT
				dim_customerdrs.drid,
				dim_customerdrs.drdate,
				dim_customerdrs.customerid,

				CASE
					WHEN staging_customerdritems.productId = '\\N' THEN NULL
					ELSE CAST(staging_customerdritems.productId AS INT)
				END,

				dim_products.unit,

				CASE
					WHEN staging_customerdritems.packageId = '\\N' THEN NULL
					ELSE CAST(staging_customerdritems.packageId AS INT)
				END,

				'product',
				CAST(staging_customerdritems.drItemQuantity AS INT),
				CAST(staging_customerdritems.itemCost AS DECIMAL(10,2)),
				CAST(staging_customerdritems.drItemDiscount1 AS DECIMAL(10,2)),
				CAST(staging_customerdritems.drItemDiscount2 AS DECIMAL(10,2)),
				ROUND((CAST(staging_customerdritems.itemCost AS DECIMAL(10,2)) * ((100 - CAST(staging_customerdritems.drItemDiscount1 AS DECIMAL(10,2)))/100)) * ((100 - CAST(staging_customerdritems.drItemDiscount2 AS DECIMAL(10,2)))/100), 2),
				CAST(staging_customerdritems.drItemQuantity AS INT) * ROUND((CAST(staging_customerdritems.itemCost AS DECIMAL(10,2)) * ((100 - CAST(staging_customerdritems.drItemDiscount1 AS DECIMAL(10,2)))/100)) * ((100 - CAST(staging_customerdritems.drItemDiscount2 AS DECIMAL(10,2)))/100), 2)

			FROM staging_customerdritems
			JOIN dim_customerdrs ON dim_customerdrs.drid = CAST(staging_customerdritems.drId AS INT)
			JOIN dim_products ON dim_products.productid = CAST(staging_customerdritems.productId AS INT)
			WHERE staging_customerdritems.packageId = '\\N'
		);

		INSERT INTO fact_item_sales
		(
			drid,
			drdate,
			customerid,
			packageid,
			type,
			quantity,
			itemcost,
			discount1,
			discount2,
			itemcost2,
			total_amount
		)

		(
			SELECT DISTINCT
				dim_customerdrs.drid,
				dim_customerdrs.drdate,
				dim_customerdrs.customerid,

				CASE
					WHEN staging_customerdritems.packageId = '\\N' THEN NULL
					ELSE CAST(staging_customerdritems.packageId AS INT)
				END,

				'package',
				CAST(staging_customerdritems.drItemQuantity AS INT),
				CAST(staging_customerdritems.itemCost AS DECIMAL(10,2)),
				CAST(staging_customerdritems.drItemDiscount1 AS DECIMAL(10,2)),
				CAST(staging_customerdritems.drItemDiscount2 AS DECIMAL(10,2)),
				ROUND((CAST(staging_customerdritems.itemCost AS DECIMAL(10,2)) * ((100 - CAST(staging_customerdritems.drItemDiscount1 AS DECIMAL(10,2)))/100)) * ((100 - CAST(staging_customerdritems.drItemDiscount2 AS DECIMAL(10,2)))/100), 2),
				CAST(staging_customerdritems.drItemQuantity AS INT) * ROUND((CAST(staging_customerdritems.itemCost AS DECIMAL(10,2)) * ((100 - CAST(staging_customerdritems.drItemDiscount1 AS DECIMAL(10,2)))/100)) * ((100 - CAST(staging_customerdritems.drItemDiscount2 AS DECIMAL(10,2)))/100), 2)

			FROM staging_customerdritems
			JOIN dim_customerdrs ON dim_customerdrs.drid = CAST(staging_customerdritems.drId AS INT)
			JOIN dim_products ON dim_products.productid = CAST(staging_customerdritems.productId AS INT)
			WHERE staging_customerdritems.packageId <> '\\N'
		);
	""")

	dim_cash_payments_insert = ("""
		INSERT INTO dim_cash_payments (
			paymentId,
			customerid,
			amount,
			cash_date,
			created_at
		)
		
		(
			SELECT 
				CAST(paymentId AS INT),
				CAST(customerid AS INT),
				CAST(paymentCashAmount AS DECIMAL(10,2)),
				CAST(paymentCashDate AS DATE),
				CAST(created_at AS TIMESTAMP)
			FROM staging_payments 
			WHERE paymentType = 'Cash'
		);
	""")

	dim_check_payments_insert = ("""
		INSERT INTO dim_check_payments (
			paymentId,
			customerid,
			amount,
			check_date,
			bank,
			check_no,
			returned,
			return_reason1,
			return_reason2,
			deposited,
			created_at
		)
		
		(
			SELECT 
				CAST(paymentId AS INT),
				CAST(customerid AS INT),
				CAST(paymentCheckAmount AS DECIMAL(10,2)),
				CAST(paymentCheckDate AS DATE),
				paymentBank,
				paymentCheckNumber,

				CASE
					WHEN paymentCheckReturned = '\\N' THEN NULL
					ELSE CAST(paymentCheckReturned AS BOOLEAN)
				END,

				paymentReturnReason1,
				paymentReturnReason2,

				CASE
					WHEN paymentCheckDeposited = '\\N' THEN NULL
					ELSE CAST(paymentCheckDeposited AS BOOLEAN)
				END,

				CAST(created_at AS TIMESTAMP)
				
			FROM staging_payments 
			WHERE paymentType = 'Check'
		);
	""")

	fact_dr_payments_insert = ("""
		INSERT INTO fact_dr_payments
		(
			paymentId,
			drid,
			customerid,
			payment_date,
			payment_type,
			drdate,
			drterms_numeric,
			areadaysallowance,
			payment_window,
			amount_paid,
			created_at
		)

		(
			SELECT
				CAST(spdr.paymentId AS INT),
				CAST(spdr.drid AS INT),
				dcp.customerid,
				dcp.cash_date,
				'cash',
				dcdr.drdate,
				dcdr.drterms_numeric,
				da.days_allowance,
				dcdr.drterms_numeric + da.days_allowance,
				CAST(spdr.paymentdrAmountPaid AS DECIMAL(10,2)),
				CAST(spdr.created_at AS TIMESTAMP)
				
			FROM staging_paymentsdrs spdr
			JOIN dim_cash_payments dcp ON dcp.paymentId = CAST(spdr.paymentId AS INT)
			JOIN dim_customerdrs dcdr ON dcdr.drid = CAST(spdr.drid AS INT)
			JOIN dim_areas da ON da.areaid = dcdr.areaid
		);

		INSERT INTO fact_dr_payments
		(
			paymentId,
			drid,
			customerid,
			payment_date,
			payment_type,
			drdate,
			drterms_numeric,
			areadaysallowance,
			payment_window,
			amount_paid,
			created_at
		)

		(
			SELECT
				CAST(spdr.paymentId AS INT),
				CAST(spdr.drid AS INT),
				dcp.customerid,
				dcp.check_date,
				'check',
				dcdr.drdate,
				dcdr.drterms_numeric,
				da.days_allowance,
				dcdr.drterms_numeric + da.days_allowance,
				CAST(spdr.paymentdrAmountPaid AS DECIMAL(10,2)),
				CAST(spdr.created_at AS TIMESTAMP)

			FROM staging_paymentsdrs spdr
			JOIN dim_check_payments dcp ON dcp.paymentId = CAST(spdr.paymentId AS INT)
			JOIN dim_customerdrs dcdr ON dcdr.drid = CAST(spdr.drid AS INT)
			JOIN dim_areas da ON da.areaid = dcdr.areaid
		);
	""")


	insert_table_queries = [
		dim_areas_insert, 
		dim_customers_insert, 
		dim_customerdrs_insert, 
		fact_customer_sales_insert, 
		dim_customerdr_dates_insert, 
		fact_customer_orders_insert,
		dim_categories_insert,
		dim_products_insert,
		dim_packages_insert,
		fact_item_sales_insert,
		dim_cash_payments_insert,
		dim_check_payments_insert,
		fact_dr_payments_insert
	]

class export_dwh_queries:
	export_dim_areas = """
		SELECT * FROM dim_areas;
	"""

	export_dim_customers = """
		SELECT * FROM dim_customers;
	"""

	export_dim_customerdrs = """
		SELECT * FROM dim_customerdrs;
	"""

	export_dim_customerdr_dates = """
		SELECT * FROM dim_customerdr_dates;
	"""

	export_fact_customer_sales = """
		SELECT * FROM fact_customer_sales;
	"""

	export_table_queries = [
		{"dim_areas": export_dim_areas},
		{"dim_customers": export_dim_customers},
		{"dim_customerdrs": export_dim_customerdrs},
		{"dim_customerdr_dates": export_dim_customerdr_dates},
		{"fact_customer_sales": export_fact_customer_sales}
	]

drop_dwhq = drop_dwh_queries()
create_dwhq = create_dwh_queries()
insert_dwhq = insert_dwh_queries()
export_dwhq = export_dwh_queries()

create_table_queries = create_dwhq.create_table_queries
drop_table_queries = drop_dwhq.drop_table_queries
insert_table_queries = insert_dwhq.insert_table_queries
export_table_queries = export_dwhq.export_table_queries