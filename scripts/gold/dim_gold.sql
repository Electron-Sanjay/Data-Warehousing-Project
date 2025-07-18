

CREATE VIEW gold.dim_customers AS
 SELECT
 	ROW_NUMBER () OVER (ORDER BY cst_id) AS customer_key,
	c.cst_id AS customer_id,
	c.cst_key AS customer_number,
	c.cst_firstname AS first_name,
	c.cst_lastname AS second_name,
	CASE
		WHEN c.cst_gndr != 'N/A' THEN c.cst_gndr -- Assuming the crm is the master source for the gndr
		ELSE COALESCE(cu.gen, 'N/A')
	END AS gender,
	c.cst_marital_status AS marital_status,
	cl.CNTRY AS country,
	cu.bdate AS birthdate,
	cu.age AS age,
	c.cst_create_date AS create_date,
	c.num_of_years_since_cust_created AS "number of years being customer"
	
FROM silver.crm_cust_info AS c
LEFT JOIN silver.erp_cust_AZ12 AS cu
ON c.cst_key = cu.cid
LEFT JOIN silver.erp_LOC_A101 AS cl
ON c.cst_key = cl.cid



CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt,pi.second_half_of_prd_key) AS product_key,
pi.prd_id AS product_id,
pi.prd_key AS product_full_key,
pi.prd_cat_id AS category_id,
pi.second_half_of_prd_key AS product_number,
pi.prd_nm AS product_name,
pc.cat AS Category,
pc.subcat AS sub_category,
pc.maintenance,
pi.prd_cost AS product_cost,
pi.prd_line AS product_line,
pi.prd_start_dt AS start_date,
CASE
    WHEN pi.prd_end_dt IS NULL THEN 
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, pi.prd_start_dt))
    ELSE 
        pi.num_of_years_prd_is_in_production
END AS years_since_production

FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_G1V2 AS pc
ON pi.prd_cat_id = pc.id
WHERE prd_end_dt IS NULL -- filtering out all the historical data

