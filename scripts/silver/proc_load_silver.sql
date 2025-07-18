-- Script for inseritng the full silver tables 


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
	ETL_start_time TIMESTAMP;
	ETL_end_time TIMESTAMP;
	start_time TIMESTAMP;
	end_time TIMESTAMP;
BEGIN
	ETL_start_time:= CLOCK_TIMESTAMP();
	
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating the existing data from the table silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info CASCADE;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating silver.crm_prd_info is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));

		
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting Prod Info to silver.crm_prd_info table';
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
		FROM bronze.crm_prd_info;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting the data into silver.crm_prd_info is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));

		
		
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating the existing data from the table silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info CASCADE;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating silver.crm_cust_info is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));


		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting Prod Info to silver.crm_cust_info table';
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
		WHERE latest =1 AND cst_id IS NOT NULL;
		
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating the existing data from the table silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details CASCADE;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating silver.crm_sales_details is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));
		
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting Prod Info to silver.crm_sales_details table';
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
		FROM bronze.crm_sales_details;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting the data into silver.crm_sales_details is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));
		
		
		
		
		
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating the existing data from the table silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12 CASCADE;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating silver.erp_cust_az12 is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));
		
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting Prod Info to silver.erp_CUST_AZ12 table';
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
		FROM bronze.erp_CUST_AZ12;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting the data into silver.erp_CUST_AZ12 is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));
		
		
		
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating the existing data from the table silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101 CASCADE;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating silver.erp_loc_a101 is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));

		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting Prod Info to silver.erp_loc_a101 table';
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
		FROM bronze.erp_loc_a101;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting the data into silver.erp_loc_a101 is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));
		
		
		
		
		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating the existing data from the table silver.erp_PX_CAT_G1V2';
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2 CASCADE;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Truncating silver.erp_PX_CAT_G1V2 is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));


		start_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting Prod Info to silver.erp_PX_CAT_G1V2 table';
		INSERT INTO silver.erp_PX_CAT_G1V2
		SELECT * FROM
		bronze.erp_PX_CAT_G1V2;
		end_time := CLOCK_TIMESTAMP();
		RAISE NOTICE 'Inserting the data into silver.erp_PX_CAT_G1V2 is  sucessfully done in: % ms', EXTRACT(EPOCH FROM (end_time - start_time));

	ETL_end_time:= CLOCK_TIMESTAMP();
	RAISE NOTICE 'ETL completed sucessfully in: %ms', EXTRACT(EPOCH FROM (ETL_end_time - ETL_start_time) );
	
		EXCEPTION
			WHEN OTHERS THEN
			RAISE NOTICE 'ERROR OCCURED';
			RAISE NOTICE 'Error Message: %', SQLERRM;
			RAISE NOTICE 'Error Code: %', SQLSTATE;
			
	
	
END;
$$;
