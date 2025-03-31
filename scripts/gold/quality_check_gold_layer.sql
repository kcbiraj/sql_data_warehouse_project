/*
Joining tables based on the similar information. 
Here, the joining is done between with tables having the customer information in those tables.
Likewise, in the scripts the count function is implemented to checking for values that have count more than 1.
*/

--============================================
/* Conducting quality checks in gold.dim_customers */
--============================================
SELECT COUNT(*) FROM
    (SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_material_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
    ON      ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
    ON      ci.cst_key = la.cid)t
    GROUP BY cst_id
    HAVING COUNT(*) > 1

-- Data Integration
SELECT DISTINCT 
    ci.cst_gndr,
    ca.gen,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender info
        ELSE COALESCE(ca.gen, 'n/a')
    END AS new_gen

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON      ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON      ci.cst_key = la.cid
ORDER BY 1, 2

-- Checking the quality checks on gold.dim_custoemrs
SELECT DISTINCT gender from gold.dim_customers;
SELECT * FROM gold.dim_customers;

--============================================
/* Conducting quality checks in gold.dim_products */
--============================================
-- Checking the quality of the data 
SELECT prd_key, COUNT(*) FROM(
SELECT 
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pc.cat,
    pc.subcat,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all the historical data
)t
GROUP BY prd_key
HAVING COUNT(*) > 1

-- Checking the final quality of Product Dimension
SELECT * FROM gold.dim_products;

--============================================
/* Conducting quality checks in gold.dim_products */
--============================================
-- Quality Check of Gold Facts (gold.fact_sales) 
SELECT 
* 
FROM gold.fact_sales;

-- Foreign Key Integrity (Dimensions)
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
