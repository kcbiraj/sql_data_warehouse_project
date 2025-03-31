/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer.';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables.';
        PRINT '------------------------------------------------';

        -- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname, -- Removing unwanted spaces from the values
        TRIM(cst_lastname) AS cst_lastname, -- Removing unwanted spaces from the values 
        CASE 
            WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
        END cst_material_status, -- Normalize (Data Normalization) marital status values to readable format
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
                ELSE 'n/a'
        END cst_gndr, -- Normalize (Data Normalization) gender values to readable format
        cst_create_date
        -- Removing the duplicate values in the data
        FROM (
            SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
            )t 
        WHERE flag_last = 1 -- Selecting the most recent record per customer
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        


        -- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
        prd_id,
        REPLACE 
            (SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extracting caterogy id (cat_id) from product key (prd_key)
            SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key, -- Extracting product key (prd_key) from product key(prd_key)
        prd_nm,
        ISNULL (prd_cost, 0) AS prd_cost,       -- Replacing NULL value in product cost with '0'
        CASE UPPER(TRIM(prd_line)) 
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
                ELSE 'n/a'    -- Mapping product line codes to descriptive values (for instance: M for Mountain)
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt, -- Removing time from the date
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 
        AS DATE
        ) 
        AS prd_end_dt -- Calculating end date as one day before the next start date
        FROM bronze.crm_prd_info
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 
            THEN NULL -- Handling Invalid Data
                ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE) -- Data Casting
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 
            THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 
            THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL 
            OR sls_sales <= 0 
            OR sls_sales != sls_quantity * ABS(sls_price) -- Handling missing data, or incorrect data by recalculating sales
            THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales/NULLIF(sls_quantity,0) -- Handling missing or incorrect data and deriving price if original is invalid
                ELSE sls_price
        END AS sls_price
        FROM bronze.crm_sales_details
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading silver.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid, 
            bdate, 
            gen
        )
        SELECT
        CASE 
            WHEN cid LIKE 'NAS%' 
            THEN SUBSTRING(cid, 4, LEN(cid)) -- Handled invalid values and removed 'NAS' prefix in the dataset
                ELSE cid
        END AS cid,
        CASE 
            WHEN bdate > GETDATE() 
            THEN NULL -- Grouping Future Birthdates as N/A
                ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(10), ''), CHAR(13), ''))) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(10), ''), CHAR(13), ''))) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a' -- Normalize gender values, handled extra spaces or newlines in the dataset and unknown cases
        END AS gen
        FROM bronze.erp_cust_az12
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT
        REPLACE(cid, '-', '') cid,
        CASE 
            WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) IN ('DE', 'Germany') THEN 'Germany'
            WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'Australia' THEN 'Australia'
            WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'Canada' THEN 'Canada'
            WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'France' THEN 'France'
            WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = 'United Kingdom' THEN 'United Kingdom'
            WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) IN ('US', 'USA', 'United States') THEN 'United States'
            WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
                ELSE cntry
        END AS cntry
        FROM bronze.erp_loc_a101
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
        id,
        cat,
        subcat,
        CASE 
            WHEN TRIM(REPLACE(REPLACE(maintenance, CHAR(10), ''), CHAR(13), '')) = 'Yes' THEN 'Yes'
            WHEN TRIM(REPLACE(REPLACE(maintenance, CHAR(10), ''), CHAR(13), '')) = 'No' THEN 'No'
        END AS maintenance
        FROM bronze.erp_px_cat_g1v2
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Loading Silver Layer is Completed.';
        PRINT '-Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='

    END TRY
    BEGIN CATCH
        PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
    END CATCH
END
