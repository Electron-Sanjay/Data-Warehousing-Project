

-- creating the schema for the different layers
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

--creating table for bronze layer

CREATE TABLE bronze.crm_cust_info (

	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status CHAR(1),
	cst_gndr CHAR(1),
	cst_create_date DATE

)

CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost NUMERIC(5,2),
	prd_line CHAR(1),
	prd_start_dt DATE,
	prd_end_dt DATE


)


CREATE TABLE bronze.crm_sales_details(
sls_ord_num VARCHAR(50),	
sls_prd_key	VARCHAR(50),
sls_cust_id VARCHAR(50),
sls_order_dt VARCHAR(50),
sls_ship_dt VARCHAR(50),
sls_due_dt VARCHAR(50),
sls_sales NUMERIC(5,2),
sls_quantity INT,
sls_price VARCHAR(50)

)

CREATE TABLE bronze.erp_cust_AZ12(
CID	VARCHAR(50),
BDATE DATE,
GEN VARCHAR(8)

)

CREATE TABLE bronze.erp_LOC_A101(
CID	VARCHAR(50),
CNTRY VARCHAR(15)

)

CREATE TABLE bronze.erp_PX_CAT_G1V2(
ID VARCHAR(20),
CAT VARCHAR(20),
SUBCAT VARCHAR(30),
MAINTENANCE VARCHAR (10)

)