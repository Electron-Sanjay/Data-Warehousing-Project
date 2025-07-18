

INSERT INTO silver.crm_cust_info(
  cst_id,
  cst_key,
  cst_firstname,
  cst_lastname,
  cst_marital_status,
  cst_gndr,
  cst_create_date,
  num_of_years_since_cust_created

)

WITH deduped AS
(
	SELECT 
	*, ROW_NUMBER() OVER (PARTITION BY (cst_id) ORDER BY cst_create_date DESC) AS LATEST
	FROM bronze.crm_cust_info
)


SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname, --removing the spaces
TRIM(cst_lastname) AS cst_lastname,--removing the spaces
CASE 
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	ELSE 'N/A'
END AS cst_marital_status, -- Normalised marital status to human readable format
CASE 
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'N/A'
END AS cst_gndr,--Normalised marital status to human readable format
cst_create_date,
ROUND(
	EXTRACT(YEAR FROM AGE(CURRENT_DATE, cst_create_date))+
	EXTRACT(MONTH FROM AGE(CURRENT_DATE, cst_create_date))/12
,2) AS num_of_years_since_cust_created -- Added this column where we can generate insights

FROM deduped
WHERE latest =1 AND cst_id IS NOT NULL


