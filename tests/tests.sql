SELECT * FROM bronze.erp_CUST_AZ12


SELECT * FROM silver.crm_sales_details

-- Verifing the the primary key has no nulls and duplicates


SELECT
cid, COUNT(*)
FROM  bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*)>1


-- TRIMING

SELECT gen
FROM bronze.erp_cust_az12
WHERE CID != TRIM(CID)

-- DATA STANDARDIZATION

SELECT
DISTINCT(gen)
FROM bronze.erp_cust_az12


--out range date
SELECT
bdate, COUNT(*)
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > CURRENT_DATE
GROUP BY 



--

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101

__
--unwanted Spaces
SELECT *
FROM bronze.erp_PX_CAT_G1V2
WHERE maintenance != TRIM(maintenance)

-- dup for pk
SELECT DISTINCT(COUNT(ID))
FROM bronze.erp_PX_CAT_G1V2
SELECT COUNT(ID)
FROM bronze.erp_PX_CAT_G1V2

-- DATA STANDARDIZATION

SELECT DISTINCT(cat) FROM bronze.erp_PX_CAT_G1V2


-- GOLD LAYER

-- checking the gen values in the cust information tables


SELECT
	
	c.cst_gndr,
	cu.gen,
	CASE
		WHEN c.cst_gndr !='N/A' THEN c.cst_gndr
		ELSE COALESCE(cu.gen, 'N/A')
	END AS new_gndr
	
FROM silver.crm_cust_info AS c
LEFT JOIN silver.erp_cust_AZ12 AS cu
ON c.cst_key = cu.cid
LEFT JOIN silver.erp_LOC_A101 AS cl
ON c.cst_key = cl.cid
WHERE c.cst_gndr = 'N/A' AND cu.gen IS NULL




