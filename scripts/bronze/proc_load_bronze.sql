/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source CSV -> Bronze Schema)
===============================================================================
Script Purpose:
    This stored procedure loads raw source data from external CSV files
    into the Bronze layer tables in PostgreSQL.

    The procedure performs the following steps:
    - Loads data from CSV files into bronze tables using the COPY command
    - Groups multiple COPY operations into a single reusable procedure
    - Provides a structured and repeatable way to ingest source data

Parameters:
    None.
    This stored procedure does not accept any input parameters and does not return values.

Usage Example:
    CALL bronze.load_bronze();

Notes:
    - CSV files are loaded from a local directory
    - Designed as part of the Bronze layer in a Data Warehouse architecture
    - Built for a PostgreSQL-based data analytics project
===============================================================================
*/


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time        TIMESTAMP;
    end_time          TIMESTAMP;
    batch_start_time  TIMESTAMP;
    batch_end_time    TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

	-- ================= crm_cust_info =================
	start_time := clock_timestamp();
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
	copy bronze.crm_cust_info
	FROM 'D:/SQL projects/sql-data-warehouse-project/dataset/source_crm/cust_info.csv'
	DELIMITER ','
	CSV
	HEADER;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';

	-- ================= crm_prd_info =================
    start_time := clock_timestamp();
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
	copy bronze.crm_prd_info
	FROM 'D:/SQL projects/sql-data-warehouse-project/dataset/source_crm/prd_info.csv'
	DELIMITER ','
	CSV
	HEADER;

	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';
	
	-- ================= crm_sales_details =================
	start_time := clock_timestamp();
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
	copy bronze.crm_sales_details
	FROM 'D:/SQL projects/sql-data-warehouse-project/dataset/source_crm/sales_details.csv'
	DELIMITER ','
	CSV
	HEADER;

	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';
	
	
	RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- ================= erp_cust_az12 =================
    start_time := clock_timestamp();
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
	copy bronze.erp_cust_az12
	FROM 'D:/SQL projects/sql-data-warehouse-project/dataset/source_erp/CUST_AZ12.csv'
	DELIMITER ','
	CSV
	HEADER;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';

	-- ================= erp_loc_a101 =================
    start_time := clock_timestamp();
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
	copy bronze.erp_loc_a101
	FROM 'D:/SQL projects/sql-data-warehouse-project/dataset/source_erp/LOC_A101.csv'
	DELIMITER ','
	CSV
	HEADER;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';

	-- ================= erp_px_cat_g1v2 =================
    start_time := clock_timestamp();
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	copy bronze.erp_px_cat_g1v2
	FROM 'D:/SQL projects/sql-data-warehouse-project/dataset/source_erp/PX_CAT_G1V2.csv'
	DELIMITER ','
	CSV
	HEADER;
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';
	
batch_end_time := clock_timestamp();

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE 'Total Load Duration: % seconds',
        EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '==========================================';

END;
$$;
