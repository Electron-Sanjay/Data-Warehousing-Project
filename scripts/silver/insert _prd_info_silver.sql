-- ETL for silver prd_info table.

INSERT INTO silver.crm_prd_info (
  prd_id,
  prd_key,
  prd_cat_id,
  second_half_of_prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt,
  num_of_years_prd_is_in_production
)


SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS prd_cat_id, -- Extract category ID
	SUBSTRING(prd_key,7,LENGTH(prd_key)) AS second_half_of_prd_key, -- Extract second half product key
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'OtherSales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line, -- Mapping product line codes to descriptive values
	prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)- INTERVAL '1 day'AS DATE) AS prd_end_dt, -- Calculating end date as one day before next start date
	ROUND(
		EXTRACT( YEAR FROM AGE(prd_end_dt,prd_start_dt)) +
		EXTRACT( MONTH FROM AGE (prd_end_dt,prd_start_dt))/12
	,2) AS num_of_years_prd_is_in_production -- calculating the number of years that product is in market for insights
FROM bronze.crm_prd_info

