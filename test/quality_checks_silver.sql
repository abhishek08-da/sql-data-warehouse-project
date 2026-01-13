/*
===============================================================================
Quality Checks – Silver Layer
===============================================================================
Script Purpose:
    This script performs data quality validation checks on the 'silver' layer 
    to ensure that data cleansing and transformation steps were successfully 
    applied.

    The checks focus on:
        - Identifying NULL or duplicate primary keys.
        - Detecting unwanted leading/trailing spaces in string columns.
        - Ensuring data standardization and consistency.
        - Validating date formats, ranges, and logical ordering.
        - Verifying consistency between related fields.

Data Quality Strategy:
    - Initial data profiling was performed on the Bronze layer to identify 
      data quality issues and inconsistencies in raw source data.
    - Silver layer validations are used to confirm that cleaning, 
      standardization, and transformation rules have been correctly applied.

Expectation:
    - Most queries in this script are expected to return NO ROWS.
    - Any returned records indicate potential data quality issues 
      that require investigation.

Usage:
    Run this script after loading data into the Silver layer.
===============================================================================
*/


-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

-- Check for NULLs or Duplicate Primary Keys
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) AS record_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL OR COUNT(*) > 1;

-- Check for Unwanted Leading/Trailing Spaces
-- Expectation: No rows
SELECT cst_firstname, cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
   OR cst_lastname <> TRIM(cst_lastname);


-- Check for Standardized Gender Values
-- Expectation: Limited standardized values (Male, Female, n/a)
SELECT DISTINCT 
    cst_gndr
FROM silver.crm_cust_info;

-- Check for Standardized Marital Status Values
-- Expectation: Consistent readable values
SELECT DISTINCT 
    cst_marital_status
FROM silver.crm_cust_info;


-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

-- Check for NULL or Duplicate Product IDs
-- Expectation: No rows
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING prd_id IS NULL OR COUNT(*) > 1;

-- Category ID derivation check
-- Expectation: cat_id should not be NULL
SELECT *
FROM silver.crm_prd_info
WHERE cat_id IS NULL;

-- Check for Standardized Product Line Values
-- Expectation: Known product line values
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Date Ranges
-- Expectation: start date <= end date OR end date IS NULL
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt IS NOT NULL
  AND prd_start_dt > prd_end_dt;


-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

-- Check for NULL Order Numbers
-- Expectation: No Results
SELECT *
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL;

-- Check for Invalid Order / Ship / Due Dates
-- Expectation: order_date <= ship_date <= due_date (when not NULL)
SELECT *
FROM silver.crm_sales_details
WHERE (sls_ship_dt IS NOT NULL AND sls_order_dt > sls_ship_dt)
   OR (sls_due_dt IS NOT NULL AND sls_ship_dt > sls_due_dt);

-- Check for Incorrect Sales Calculation
-- Expectation: sls_sales = quantity * price
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price;

-- Negative or zero values check
-- Expectation: No rows
SELECT *
FROM silver.crm_sales_details
WHERE sls_quantity <= 0
   OR sls_price <= 0;


-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

-- Check for NULL Customer IDs
-- Expectation: No Results
SELECT *
FROM silver.erp_cust_az12
WHERE cid IS NULL;

-- Check for Invalid Birth Dates (Future Dates)
-- Expectation: No Results
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > CURRENT_DATE;

-- Check for Standardized Gender Values
-- Expectation: Male, Female, n/a
SELECT DISTINCT gen
FROM silver.erp_cust_az12;


-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

-- Check for Unwanted Spaces in Country
-- Expectation: No Results
SELECT *
FROM silver.erp_loc_a101
WHERE cntry <> TRIM(cntry);

-- Check for Country Standardization
-- Expectation: Clean country names
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;


-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Check for NULL Category IDs
-- Expectation: No Results
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE id IS NULL;

-- Check for Duplicate Category IDs
-- Expectation: No Results
SELECT
    id,
    COUNT(*)
FROM silver.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1;

-- Check for Missing Category or Subcategory
-- Expectation: No Results
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat IS NULL
   OR subcat IS NULL;
