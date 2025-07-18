-- Silver layer for the sales details.





INSERT INTO silver.crm_sales_details(
	sls_ord_num ,	
	sls_prd_key	,
	sls_cust_id ,
	sls_order_dt ,
	sls_ship_dt ,
	sls_due_dt ,
	sls_sales ,
	sls_quantity,
	sls_price 
	
)

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
	WHEN sls_order_dt = '00000000' OR LENGTH(sls_order_dt) !=8 THEN NULL
	ELSE TO_DATE(sls_order_dt,'YYYYMMDD')
END AS sls_order_dt,
CASE 
	WHEN sls_ship_dt = '00000000' OR LENGTH(sls_ship_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE 
	WHEN sls_due_dt = '00000000' OR LENGTH(sls_due_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE
	WHEN sls_sales IS NULL OR sls_sales<='0' OR sls_sales != sls_quantity *ABS(CAST(sls_price AS NUMERIC)) THEN sls_quantity * ABS(CAST(sls_price AS NUMERIC))
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE
	WHEN sls_price is NULL OR CAST(sls_price AS NUMERIC) <=0 THEN CAST(sls_sales AS NUMERIC)/CAST(COALESCE(sls_quantity,0) AS NUMERIC)
	ELSE CAST(sls_price AS NUMERIC)
END AS sls_price
FROM bronze.crm_sales_details