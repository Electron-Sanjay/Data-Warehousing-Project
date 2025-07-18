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



