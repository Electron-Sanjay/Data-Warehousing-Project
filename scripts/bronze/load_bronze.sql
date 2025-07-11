
--creating the stored procedure to bulk insert the data to get the new data into the erp/crm
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
BEGIN
	start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '=========================================';
		RAISE NOTICE 'Loading Bronze Layer';
		RAISE NOTICE '=========================================';
		-- bulk inserting the data from the local machine
		--truncate and loading the table to avoid the duplicates
		RAISE NOTICE '___________________________';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '___________________________';

		
		RAISE NOTICE 'Truncating TABLE: bronze.crm_cust_info';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.crm_cust_info;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_cust_info truncated in: %', EXTRACT(EPOCH FROM end_time - start_time);

		
		RAISE NOTICE 'Coping.. data into  TABLE: bronze.crm_cust_info';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.crm_cust_info
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_crm\cust_info.csv'
		WITH(
		 FORMAT csv,
		 HEADER,
		 DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_cust_info inserted in: %', EXTRACT(EPOCH FROM end_time - start_time);
		

		
		RAISE NOTICE 'Truncating TABLE: bronze.crm_prd_info';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.crm_prd_info;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_prd_info truncated in: %', EXTRACT(EPOCH FROM end_time - start_time);
		
		RAISE NOTICE 'Coping.. data into  TABLE: bronze.crm_prd_info';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.crm_prd_info
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_crm\prd_info.csv'
		WITH (
		FORMAT csv,
		HEADER,
		DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_prd_info inserted in: %', EXTRACT(EPOCH FROM end_time - start_time);
		
		
		RAISE NOTICE 'Truncating TABLE: bronze.crm_sales_details';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.crm_sales_details;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_sales_details were truncated in: %', EXTRACT(EPOCH FROM end_time - start_time);
		
		RAISE NOTICE 'Coping.. data into  TABLE: bronze.crm_sales_details';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.crm_sales_details
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_crm\sales_details.csv'
		WITH(
		FORMAT csv,
		HEADER,
		DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_sales_details were copied in: %', EXTRACT(EPOCH FROM end_time - start_time);
		

		
		RAISE NOTICE '___________________________';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '___________________________';

		
		RAISE NOTICE 'Truncating TABLE: bronze.erp_CUST_AZ12';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_CUST_AZ12 were truncated in: %', EXTRACT(EPOCH FROM end_time - start_time);
		
		
		RAISE NOTICE 'Coping.. data into  TABLE: bronze.erp_CUST_AZ12';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.erp_CUST_AZ12
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FORMAT csv,
		HEADER,
		DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_CUST_AZ12 were inserted in: %', EXTRACT(EPOCH FROM end_time - start_time);

		
		RAISE NOTICE 'Truncating TABLE: bronze.erp_LOC_A101';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.erp_LOC_A101;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_LOC_A101 is truncated in: %', EXTRACT(EPOCH FROM end_time - start_time);
		
		
		RAISE NOTICE 'Coping.. data into  TABLE: bronze.erp_LOC_A101';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.erp_LOC_A101
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FORMAT csv,
		HEADER,
		DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_LOC_A101 were inserted in: %', EXTRACT(EPOCH FROM end_time - start_time);

		
		RAISE NOTICE 'Truncating TABLE: bronze.erp_PX_CAT_G1V2';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_PX_CAT_G1V2 is truncated in: %', EXTRACT(EPOCH FROM end_time -start_time);
		
		
		RAISE NOTICE 'Coping.. data into TABLE: bronze.erp_PX_CAT_G1V2';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.erp_PX_CAT_G1V2
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FORMAT csv,
		HEADER,
		DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_PX_CAT_G1V2 were inserted in: %', EXTRACT(EPOCH FROM end_time -start_time);
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'ETL process completed successfully in: %', EXTRACT(EPOCH FROM end_time - start_time);	
	EXCEPTION
		WHEN OTHERS THEN
		RAISE NOTICE 'ERROR OCCURED';
		RAISE NOTICE 'Error Message: %', SQLERRM;
		RAISE NOTICE 'Error Code: %', SQLSTATE;
		
	
END;
$$;

--creating the stored procedure to bulk insert the data to get the new data into the erp/crm
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time TIMESTAMP;
	end_time TIMESTAMP;
BEGIN
	start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '=========================================';
		RAISE NOTICE 'Loading Bronze Layer';
		RAISE NOTICE '=========================================';
		-- bulk inserting the data from the local machine
		--truncate and loading the table to avoid the duplicates
		RAISE NOTICE '___________________________';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '___________________________';

		
		RAISE NOTICE 'Truncating TABLE: bronze.crm_cust_info';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.crm_cust_info;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_cust_info truncated in: %', EXTRACT(EPOCH FROM end_time - start_time);

		
		RAISE NOTICE 'Coping.. data into  TABLE: bronze.crm_cust_info';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.crm_cust_info
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_crm\cust_info.csv'
		WITH(
		 FORMAT csv,
		 HEADER,
		 DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_cust_info inserted in: %', EXTRACT(EPOCH FROM end_time - start_time);
		

		
		RAISE NOTICE 'Truncating TABLE: bronze.crm_prd_info';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.crm_prd_info;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_prd_info truncated in: %', EXTRACT(EPOCH FROM end_time - start_time);
		
		RAISE NOTICE 'Coping.. data into  TABLE: bronze.crm_prd_info';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.crm_prd_info
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_crm\prd_info.csv'
		WITH (
		FORMAT csv,
		HEADER,
		DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_prd_info inserted in: %', EXTRACT(EPOCH FROM end_time - start_time);
		
		
		RAISE NOTICE 'Truncating TABLE: bronze.crm_sales_details';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.crm_sales_details;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_sales_details were truncated in: %', EXTRACT(EPOCH FROM end_time - start_time);
		
		RAISE NOTICE 'Coping.. data into  TABLE: bronze.crm_sales_details';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.crm_sales_details
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_crm\sales_details.csv'
		WITH(
		FORMAT csv,
		HEADER,
		DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.crm_sales_details were copied in: %', EXTRACT(EPOCH FROM end_time - start_time);
		

		
		RAISE NOTICE '___________________________';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '___________________________';

		
		RAISE NOTICE 'Truncating TABLE: bronze.erp_CUST_AZ12';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_CUST_AZ12 were truncated in: %', EXTRACT(EPOCH FROM end_time - start_time);
		
		
		RAISE NOTICE 'Coping.. data into  TABLE: bronze.erp_CUST_AZ12';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.erp_CUST_AZ12
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FORMAT csv,
		HEADER,
		DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_CUST_AZ12 were inserted in: %', EXTRACT(EPOCH FROM end_time - start_time);

		
		RAISE NOTICE 'Truncating TABLE: bronze.erp_LOC_A101';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.erp_LOC_A101;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_LOC_A101 is truncated in: %', EXTRACT(EPOCH FROM end_time - start_time);
		
		
		RAISE NOTICE 'Coping.. data into  TABLE: bronze.erp_LOC_A101';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.erp_LOC_A101
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FORMAT csv,
		HEADER,
		DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_LOC_A101 were inserted in: %', EXTRACT(EPOCH FROM end_time - start_time);

		
		RAISE NOTICE 'Truncating TABLE: bronze.erp_PX_CAT_G1V2';
		start_time := CURRENT_TIMESTAMP;
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_PX_CAT_G1V2 is truncated in: %', EXTRACT(EPOCH FROM end_time -start_time);
		
		
		RAISE NOTICE 'Coping.. data into TABLE: bronze.erp_PX_CAT_G1V2';
		start_time := CURRENT_TIMESTAMP;
		COPY bronze.erp_PX_CAT_G1V2
		FROM 'E:\Knowledge\Projects\Data\end to end datawarehouse project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FORMAT csv,
		HEADER,
		DELIMITER ','
		);
		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE 'bronze.erp_PX_CAT_G1V2 were inserted in: %', EXTRACT(EPOCH FROM end_time -start_time);
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'ETL process completed successfully in: %', EXTRACT(EPOCH FROM end_time - start_time);	
	EXCEPTION
		WHEN OTHERS THEN
		RAISE NOTICE 'ERROR OCCURED';
		RAISE NOTICE 'Error Message: %', SQLERRM;
		RAISE NOTICE 'Error Code: %', SQLSTATE;
		
	
END;
$$;

call bronze.load_bronze()