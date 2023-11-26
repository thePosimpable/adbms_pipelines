import configparser, os

config = configparser.ConfigParser()
config.read('./pipeline/config/appconfigs.cfg')

DHDC_DESTDIR_PATH = config.get('MYSQL', 'DHDC_DESTDIR_PATH')
EXCELHOME_DESTDIR_PATH = config.get('MYSQL', 'EXCELHOME_DESTDIR_PATH')

class export_mysql_queries:
	export_areas = """
		SELECT 
			"areaid",
			"arearepname",
			"arealocation",
			"created_at",
			"updated_at",
			"areadaysallowance"

		UNION ALL

		SELECT 
			areaid,
			arearepname,
			arealocation,
			created_at,
			updated_at,
			areadaysallowance

		FROM areas		
		INTO OUTFILE '%sareas.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ","
		LINES TERMINATED BY '\n';
	"""

	export_customers = """
		SELECT 
			"customerid",
			"customerName",
			"customerAddress",
			"customerContactNumber",
			"areaid",
			"created_at",
			"updated_at",
			"customerAccountBalance"

		UNION ALL

		SELECT 
			customerid,
			customerName,
			customerAddress,
			customerContactNumber,
			areaid,
			created_at,
			updated_at,
			customerAccountBalance

		FROM customers		
		INTO OUTFILE '%scustomers.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ","
		LINES TERMINATED BY '\n';
	"""

	export_customerdrs = """
		SELECT 
			"drid",
			"drno",
			"drdate",
			"dramount",
			"drbalance",
			"drstatus",
			"drterms",
			"customerid",
			"areaid",
			"created_at",
			"updated_at"

		UNION ALL

		SELECT 
			drid,
			drno,
			drdate,
			dramount,
			drbalance,
			drstatus,
			ifnull(drterms, ""),
			customerid,
			areaid,
			created_at,
			updated_at

		FROM deliveryreceipts		
		INTO OUTFILE '%scustomerdrs.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ","
		LINES TERMINATED BY '\n';
	"""

	export_dritems = """
		SELECT 
			"dritemid",
			"drId",
			"productId",
			"packageId",
			"drItemQuantity",
			"itemCost",
			"drItemDiscount1",
			"drItemDiscount2",
			"created_at",
			"updated_at"

		UNION ALL

		SELECT 
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

		FROM dritems		
		INTO OUTFILE '%sdritems.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ","
		LINES TERMINATED BY '\n';
	"""

	export_products = """
		SELECT 
			"productid",
			"unitId",
			"productName",
			"productSize",
			"productSellingPrice",
			"productBuyingPrice",
			"productQuantity",
			"created_at",
			"updated_at",
			"productDiscount1",
			"productDiscount2",
			"productInIB",
			"productCode"

		UNION ALL

		SELECT 
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

		FROM products
		INTO OUTFILE '%sproducts.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ";"
		LINES TERMINATED BY '\n';
	"""

	export_packages = """
		SELECT 
			"packageid",
			"packageName",
			"packageAmount",
			"packageSellingPrice",
			"created_at",
			"updated_at",
			"packageBuyingPrice",
			"packageCode"

		UNION ALL

		SELECT 
			packageid,
			packageName,
			packageAmount,
			packageSellingPrice,
			created_at,
			updated_at,
			packageBuyingPrice,
			packageCode

		FROM packages		
		INTO OUTFILE '%spackages.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ","
		LINES TERMINATED BY '\n';
	"""

	export_uoms = """
		SELECT 
			"unitid",
			"unitName",
			"created_at",
			"updated_at"

		UNION ALL

		SELECT 
			unitid,
			unitName,
			created_at,
			updated_at

		FROM unitofmeasurements		
		INTO OUTFILE '%suoms.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ","
		LINES TERMINATED BY '\n';
	"""

	export_categories = """
		SELECT 
			"categoryid",
			"categoryName",
			"created_at",
			"updated_at"

		UNION ALL

		SELECT 
			categoryid,
			categoryName,
			created_at,
			updated_at

		FROM categories		
		INTO OUTFILE '%scategories.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ","
		LINES TERMINATED BY '\n';
	"""

	export_productcategories = """
		SELECT 
			"productcategoryid",
			"productId",
			"categoryId",
			"created_at",
			"updated_at"

		UNION ALL

		SELECT 
			productcategoryid,
			productId,
			categoryId,
			created_at,
			updated_at

		FROM productcategories		
		INTO OUTFILE '%sproductcategories.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ","
		LINES TERMINATED BY '\n';
	"""

	export_payments = """
		SELECT 
			"paymentId",
			"customerid",
			"paymentCheckDate",
			"paymentCashDate",
			"paymentBank",
			"paymentCheckNumber",
			"paymentCashAmount",
			"paymentCheckAmount",
			"created_at",
			"updated_at",
			"paymentCleared",
			"paymentType",
			"paymentCheckReturned",
			"paymentReturnReason1",
			"paymentReturnReason2",
			"paymentCheckDeposited"

		UNION ALL

		SELECT 
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

		FROM payments		
		INTO OUTFILE '%spayments.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ","
		LINES TERMINATED BY '\n';
	"""

	export_paymentsdrs = """
		SELECT 
			"paymentdrId",
			"drid",
			"paymentId",
			"paymentdrAmountPaid",
			"created_at",
			"updated_at",
			"paymentdrDrBalance",
			"paymentdrDrStatus",
			"rsId",
			"pasId"

		UNION ALL

		SELECT 
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

		FROM paymentsdrs		
		INTO OUTFILE '%spaymentsdrs.csv' 
		FIELDS ENCLOSED BY '"' TERMINATED BY ","
		LINES TERMINATED BY '\n';
	"""

	def adjust_export_queries(self, path):
		return list(
			map(
				lambda q: q % (path), 
				[
					self.export_areas,
					self.export_customers, 
					self.export_customerdrs,
					self.export_dritems,
					self.export_products,
					self.export_packages,
					self.export_uoms,
					self.export_categories,
					self.export_productcategories,
					self.export_payments,
					self.export_paymentsdrs
				]
			)
		)

export_mq = export_mysql_queries()

export_queries = {
	'DHDC_DB': export_mq.adjust_export_queries(DHDC_DESTDIR_PATH),
	'EXCELHOME_DB': export_mq.adjust_export_queries(EXCELHOME_DESTDIR_PATH),
}