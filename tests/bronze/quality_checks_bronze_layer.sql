

-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- Checking for unwanted spaces in the datasets
-- Expectation: No Results
SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key)

-- Data Standardization and Consistency
SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info
