INSERT INTO silver.erp_cust_az12(
CID	,
BDATE ,
GEN ,
AGE 
)
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE CID
END AS CID,
CASE 
	WHEN bdate > CURRENT_DATE THEN NULL
	ELSE bdate
END AS bdate,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	ELSE 'N/A'
END AS gen,
EXTRACT(YEAR FROM AGE(CURRENT_DATE,bdate)) AS Age

FROM bronze.erp_CUST_AZ12

SELECT * FROM SILVER.erp_cust_az12