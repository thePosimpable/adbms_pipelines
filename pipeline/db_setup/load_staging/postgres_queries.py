from pipeline.utils import generate_postgres_drop_queries

class create_staging_queries:
	create_staging_areas = """
		CREATE TABLE IF NOT EXISTS staging_areas (
			areaid TEXT,
			arearepname TEXT,
			arealocation TEXT,
			created_at TEXT,
			updated_at TEXT,
			areadaysallowance TEXT
		);
	"""

	create_staging_customers = """
		CREATE TABLE IF NOT EXISTS staging_customers (
			customerid TEXT,
			customerName TEXT,
			customerAddress TEXT,
			customerContactNumber TEXT,
			areaid TEXT,
			created_at TEXT,
			updated_at TEXT,
			customerAccountBalance TEXT
		);
	"""

	create_staging_customerdrs = """
		CREATE TABLE IF NOT EXISTS staging_customerdrs (
			drid TEXT,
			drno TEXT,
			drdate TEXT,
			dramount TEXT,
			drbalance TEXT,
			drstatus TEXT,
			drterms TEXT,
			customerid TEXT,
			areaid TEXT,
			created_at TEXT,
			updated_at TEXT
		);
	"""

	create_staging_customerdritems = """
		CREATE TABLE IF NOT EXISTS staging_customerdritems (
			dritemid TEXT,
			drId TEXT,
			productId TEXT,
			packageId TEXT,
			drItemQuantity TEXT,
			itemCost TEXT,
			drItemDiscount1 TEXT,
			drItemDiscount2 TEXT,
			created_at TEXT,
			updated_at TEXT
		);
	"""

	create_staging_products = """
		CREATE TABLE IF NOT EXISTS staging_products (
			productid TEXT,
			unitId TEXT,
			productName TEXT,
			productSize TEXT,
			productSellingPrice TEXT,
			productBuyingPrice TEXT,
			productQuantity TEXT,
			created_at TEXT,
			updated_at TEXT,
			productDiscount1 TEXT,
			productDiscount2 TEXT,
			productInIB TEXT,
			productCode TEXT
		);
	"""

	create_staging_packages = """
		CREATE TABLE IF NOT EXISTS staging_packages (
			packageid TEXT,
			packageName TEXT,
			packageAmount TEXT,
			packageSellingPrice TEXT,
			created_at TEXT,
			updated_at TEXT,
			packageBuyingPrice TEXT,
			packageCode TEXT
		);
	"""

	create_staging_uoms = """
		CREATE TABLE IF NOT EXISTS staging_uoms (
			unitid TEXT,
			unitName TEXT,
			created_at TEXT,
			updated_at TEXT
		);
	"""

	create_staging_categories = """
		CREATE TABLE IF NOT EXISTS staging_categories (
			categoryid TEXT,
			categoryName TEXT,
			created_at TEXT,
			updated_at TEXT
		);
	"""

	create_staging_productcategories = """
		CREATE TABLE IF NOT EXISTS staging_productcategories (
			productcategoryid TEXT,
			productId TEXT,
			categoryId TEXT,
			created_at TEXT,
			updated_at TEXT
		);
	"""

	create_staging_payments = """
		CREATE TABLE IF NOT EXISTS staging_payments (
			paymentId TEXT,
			customerid TEXT,
			paymentCheckDate TEXT,
			paymentCashDate TEXT,
			paymentBank TEXT,
			paymentCheckNumber TEXT,
			paymentCashAmount TEXT,
			paymentCheckAmount TEXT,
			created_at TEXT,
			updated_at TEXT,
			paymentCleared TEXT,
			paymentType TEXT,
			paymentCheckReturned TEXT,
			paymentReturnReason1 TEXT,
			paymentReturnReason2 TEXT,
			paymentCheckDeposited TEXT
		);
	"""

	create_staging_paymentsdrs = """
		CREATE TABLE IF NOT EXISTS staging_paymentsdrs (
			paymentdrId TEXT,
			drid TEXT,
			paymentId TEXT,
			paymentdrAmountPaid TEXT,
			created_at TEXT,
			updated_at TEXT,
			paymentdrDrBalance TEXT,
			paymentdrDrStatus TEXT,
			rsId TEXT,
			pasId TEXT
		);
	"""

	create_table_queries = [
		create_staging_areas, 
		create_staging_customers, 
		create_staging_customerdrs, 
		create_staging_customerdritems, 
		create_staging_products, 
		create_staging_packages,
		create_staging_uoms,
		create_staging_categories,
		create_staging_productcategories,
		create_staging_payments,
		create_staging_paymentsdrs
	]

class copy_staging_queries:
	staging_areas_copy = ("""
		COPY %s (
			areaid,
			arearepname,
			arealocation,
			created_at,
			updated_at,
			areadaysallowance
		)

		FROM STDIN
		WITH NULL AS ' ' CSV
		HEADER;
	""")

	staging_customers_copy = ("""
		COPY %s (
			customerid,
			customerName,
			customerAddress,
			customerContactNumber,
			areaid,
			created_at,
			updated_at,
			customerAccountBalance
		)

		FROM STDIN
		WITH NULL AS ' ' CSV
		HEADER;
	""")

	staging_customerdrs_copy = ("""
		COPY %s (
			drid,
			drno,
			drdate,
			dramount,
			drbalance,
			drstatus,
			drterms,
			customerid,
			areaid,
			created_at,
			updated_at
		)

		FROM STDIN
		WITH NULL AS ' ' CSV
		HEADER;
	""")

	staging_customerdritems_copy = ("""
		COPY %s (
			dritemid,
			drId,
			productId,
			packageId,
			drItemQuantity,
			itemCost,
			drItemDiscount1,
			drItemDiscount2,
			created_at,
			updated_at
		)

		FROM STDIN
		WITH NULL AS ' ' CSV
		HEADER;
	""")

	staging_products_copy = ("""
		COPY %s (
			productid,
			unitId,
			productName,
			productSize,
			productSellingPrice,
			productBuyingPrice,
			productQuantity,
			created_at,
			updated_at,
			productDiscount1,
			productDiscount2,
			productInIB,
			productCode
		)

		FROM STDIN
		WITH 
			CSV
			HEADER
			DELIMITER ';'
			NULL AS ' '
			QUOTE '"' ESCAPE '\\' 
		;
	""")

	staging_packages_copy = ("""
		COPY %s (
			packageid,
			packageName,
			packageAmount,
			packageSellingPrice,
			created_at,
			updated_at,
			packageBuyingPrice,
			packageCode
		)

		FROM STDIN
		WITH NULL AS ' ' CSV
		HEADER;
	""")

	staging_uoms_copy = ("""
		COPY %s (
			unitid,
			unitName,
			created_at,
			updated_at
		)

		FROM STDIN
		WITH NULL AS ' ' CSV
		HEADER;
	""")

	staging_categories_copy = ("""
		COPY %s (
			categoryid,
			categoryName,
			created_at,
			updated_at
		)

		FROM STDIN
		WITH NULL AS ' ' CSV
		HEADER;
	""")

	staging_productcategories_copy = ("""
		COPY %s (
			productcategoryid,
			productId,
			categoryId,
			created_at,
			updated_at
		)

		FROM STDIN
		WITH NULL AS ' ' CSV
		HEADER;
	""")

	staging_payments_copy = ("""
		COPY %s (
			paymentId,
			customerid,
			paymentCheckDate,
			paymentCashDate,
			paymentBank,
			paymentCheckNumber,
			paymentCashAmount,
			paymentCheckAmount,
			created_at,
			updated_at,
			paymentCleared,
			paymentType,
			paymentCheckReturned,
			paymentReturnReason1,
			paymentReturnReason2,
			paymentCheckDeposited
		)

		FROM STDIN
		WITH NULL AS ' ' CSV
		HEADER;
	""")

	staging_paymentsdrs_copy = ("""
		COPY %s (
			paymentdrId,
			drid,
			paymentId,
			paymentdrAmountPaid,
			created_at,
			updated_at,
			paymentdrDrBalance,
			paymentdrDrStatus,
			rsId,
			pasId
		)

		FROM STDIN
		WITH NULL AS ' ' CSV
		HEADER;
	""")

	CSV_MAPPING = {
		"areas.csv": {
			"query": staging_areas_copy,
			"table": "staging_areas"
		},

		"customers.csv": {
			"query": staging_customers_copy,
			"table": "staging_customers"
		},

		"customerdrs.csv": {
			"query": staging_customerdrs_copy,
			"table": "staging_customerdrs"
		},

		"dritems.csv": {
			"query": staging_customerdritems_copy,
			"table": "staging_customerdritems"
		},

		"products.csv": {
			"query": staging_products_copy,
			"table": "staging_products"
		},

		"packages.csv": {
			"query": staging_packages_copy,
			"table": "staging_packages"
		},

		"uoms.csv": {
			"query": staging_uoms_copy,
			"table": "staging_uoms"
		},

		"categories.csv": {
			"query": staging_categories_copy,
			"table": "staging_categories"
		},

		"productcategories.csv": {
			"query": staging_productcategories_copy,
			"table": "staging_productcategories"
		},

		"payments.csv": {
			"query": staging_payments_copy,
			"table": "staging_payments"
		},

		"paymentsdrs.csv": {
			"query": staging_paymentsdrs_copy,
			"table": "staging_paymentsdrs"
		}
	}

class drop_staging_queries:
	staging_tables = [
		"staging_areas",
		"staging_customers",
		"staging_customerdrs",
		"staging_customerdritems",
		"staging_products",
		"staging_packages",
		"staging_uoms",
		"staging_categories",
		"staging_productcategories",
		"staging_payments",
		"staging_paymentsdrs"
	]

	drop_table_queries = generate_postgres_drop_queries(staging_tables)

create_sq = create_staging_queries()
copy_sq = copy_staging_queries()
drop_sq = drop_staging_queries()

create_table_queries = create_sq.create_table_queries
drop_table_queries = drop_sq.drop_table_queries
CSV_MAPPING = copy_sq.CSV_MAPPING