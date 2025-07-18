SELECT * FROM silver.erp_loc_a101


INSERT INTO silver.erp_loc_a101(
cid,
cntry

)
SELECT
REPLACE(cid,'-','')cid,
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101