/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, and modify as per requirement.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE,
    dwh_create_date     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE silver.crm_prd_info
ADD COLUMN cat_id VARCHAR(50);


CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE silver.crm_sales_details
ALTER COLUMN sls_order_dt TYPE DATE
USING to_date(sls_order_dt::text, 'YYYYMMDD');

ALTER TABLE silver.crm_sales_details
ALTER COLUMN sls_ship_dt TYPE DATE
USING to_date(sls_ship_dt::text, 'YYYYMMDD');

ALTER TABLE silver.crm_sales_details
ALTER COLUMN sls_due_dt TYPE DATE
USING to_date(sls_due_dt::text, 'YYYYMMDD');



CREATE TABLE silver.erp_loc_a101 (
    cid   VARCHAR(50),
    cntry VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE silver.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE silver.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
