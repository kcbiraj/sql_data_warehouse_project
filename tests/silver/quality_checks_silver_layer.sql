/*
======================================================================
Quality Checks
======================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy, and standardization across the ‘silver’ schema. It includes checks for:
-	Null or Duplicate primary keys
-	Unwanted spaces in string fields
-	Data standardization and consistency
-	Invalid date ranges and orders
-	Data consistency between related fields
Usage Notes:
-	Run these checks after loading Silver Layer
-	Investigate and resolve any discrepancies found during the quality checks
======================================================================
*/

/* Quality Checks in Silver Layer */

-- Check For Nulls or Duplicates in Primary Key (silver.crm_cust_info)
-- Expectation: No Result
SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- Checking for unwanted spaces in the datasets
-- Expectation: No Results
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key)


SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


-- Data Standardization and Consistency
SELECT DISTINCT cst_material_status, cst_gndr
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info;


-- Check For Nulls or Duplicates in Primary Key in CRM Product Info (silver.crm_prd_info)
-- Expectation: No Result
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- Checking for unwanted spaces in the datasets
-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


-- Data Standardization and Consistency
SELECT DISTINCT cst_material_status, cst_gndr
FROM silver.crm_cust_info

SELECT * FROM silver.crm_prd_info

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL


-- Checking for Invalid Dates in Sales Details (silver.crm_sales_details)
SELECT
--sls_order_dt
NULLIF (sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

--Checking for Invalid Orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Checking Data Consistency: Between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be Null, Zero or Negative
SELECT DISTINCT
sls_sales,
sls_quantity, 
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULl OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- Transforming the Data of sls_sales, sls_quantity, sls_price
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity, 
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales/NULLIF(sls_quantity,0)
    ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULl OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


-- Transforming CID and Validating the data with Silver.crm_cust_info table
-- Execution: No result
SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
    ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- Identify Out-of-Ranges-Dates
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1925-01-01' OR bdate > GETDATE()

-- Data Standardization and Consistency
SELECT DISTINCT
gen
FROM bronze.erp_cust_az12

-- Check for Hidden Newlines in dataset
SELECT DISTINCT
gen,
LEN(gen) AS length,
LEN(TRIM(gen)) AS trimmed_length
FROM bronze.erp_cust_az12

-- Checking Data Quality in Silver.erp_cust_az12 table
-- Identify Out-of-Ranges-Dates
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1925-01-01' OR bdate > GETDATE()

-- Data Standardization and Consistency
SELECT DISTINCT
gen
FROM silver.erp_cust_az12

-- Check for Hidden Newlines in dataset
SELECT DISTINCT
gen,
LEN(gen) AS length,
LEN(TRIM(gen)) AS trimmed_length
FROM silver.erp_cust_az12

-- Final Check on Silver.erp_cust_az12
SELECT * FROM silver.erp_cust_az12;


-- Queries to check, handle errors in the data silver.erp_loc_a101
SELECT
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key from silver.crm_cust_info)

-- Data Standardization and Consistency
SELECT DISTINCT 
cntry
FROM bronze.erp_loc_a101

-- Cleaning the column cntry for unwanted spaces, missing values, and data consistency
SELECT DISTINCT
cntry AS old_cntry,
CASE 
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) IN ('DE', 'Germany') THEN 'Germany'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'Australia' THEN 'Australia'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'Canada' THEN 'Canada'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'France' THEN 'France'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'United Kingdom' THEN 'United Kingdom'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) IN ('US', 'USA', 'United States') THEN 'United States'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
        ELSE cntry
    END AS cleaned_cntry
    FROM bronze.erp_loc_a101

-- Checking for the trimmed values after data transformation in column cntry
SELECT DISTINCT cntry, 
       LEN(cntry) AS original_length, 
       LEN(TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), ''))) AS cleaned_length
FROM bronze.erp_loc_a101
WHERE LEN(cntry) != LEN(TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')));


-- After transformation and loading data into Silver.erp_loc_a101
-- Data Standardization and Consistency
SELECT DISTINCT 
cntry
FROM silver.erp_loc_a101

-- Cleaning the column cntry for unwanted spaces, missing values, and data consistency
SELECT DISTINCT
cntry AS old_cntry,
CASE 
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) IN ('DE', 'Germany') THEN 'Germany'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'Australia' THEN 'Australia'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'Canada' THEN 'Canada'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'France' THEN 'France'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'United Kingdom' THEN 'United Kingdom'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) IN ('US', 'USA', 'United States') THEN 'United States'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
        ELSE cntry
    END AS cleaned_cntry
    FROM silver.erp_loc_a101

-- Final Check of the table Silver.erp_loc_a101
SELECT * FROM silver.erp_loc_a101;


-- Checking quality checks in silver.erp_px_cat_g1v2
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

-- Checking for unwanted Spaces 
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Checking for Data Standardization and Consistency
SELECT DISTINCT
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

-- Checking for Unwanted Spaces and Lengths in Maintenance Columns 
SELECT DISTINCT
maintenance AS old_maintenance,
CASE
    WHEN TRIM(REPLACE(REPLACE(maintenance, CHAR(10),''), CHAR(13),'')) = 'Yes' THEN 'Yes'
    WHEN TRIM(REPLACE(REPLACE(maintenance, CHAR(10),''), CHAR(13),'')) = 'No' THEN 'No'
END AS maintenance
FROM bronze.erp_px_cat_g1v2;

-- Checking for unwanted lengths
SELECT DISTINCT maintenance, 
       LEN(maintenance) AS original_length, 
       LEN(TRIM(REPLACE(REPLACE(maintenance, CHAR(10), ''), CHAR(13), ''))) AS cleaned_length
FROM bronze.erp_px_cat_g1v2
WHERE LEN(maintenance) != LEN(TRIM(REPLACE(REPLACE(maintenance, CHAR(10), ''), CHAR(13), '')));

-- Final check on Silver Layer (silver.erp_px_cat_g1v2)
SELECT
* 
FROM silver.erp_px_cat_g1v2;
