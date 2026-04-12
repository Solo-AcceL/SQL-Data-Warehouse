-- INSERT INTO SILVER TABLES
SELECT '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
SELECT '>> Inserting Data Into: silver.crm_cust_info';
INSERT INTO silver.crm_cust_info (
	cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT cst_id,
	   cst_key,
       TRIM(cst_firstname) AS cst_firstname,
       TRIM(cst_lastname) AS cst_lastname,
       CASE WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
			WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
            ELSE 'n/a'
		END cst_marital_status,
       CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
			WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
            ELSE 'n/a'
		END cst_gndr,
       cst_create_date
FROM (
	SELECT *,
		   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last	
	FROM bronze.crm_cust_info 
	) t
WHERE flag_last = 1 AND cst_id IS NOT NULL;

-- Check for duplicates
SELECT cst_id, COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces in strings
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT *
FROM silver.crm_cust_info;

SELECT * FROM bronze.crm_prd_info;

-- Check for duplicates and nulls
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 and prd_id IS NULL;

-- INSERT INTO SILVER TABLES
SELECT '>> Truncating Table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
SELECT '>> Inserting Data Into: silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT prd_id,
       REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
       SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
       prd_nm,
       IFNULL(prd_cost, 0) AS prd_cost,
       CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
		END AS prd_line,
       CAST(prd_start_dt AS DATE) AS prd_start_dt,
	   CAST(DATE_SUB(
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
        INTERVAL 1 DAY) AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

-- WHERE SUBSTRING(prd_key, 7, LENGTH(prd_key)) IN (
-- 	SELECT sls_prd_key FROM bronze.crm_sales_details
-- );

-- WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (
-- 	SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2
-- );

-- Check for unwanted spaces
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLS or Negative numbers
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0;

-- Check date columns
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 and prd_id IS NULL;

SELECT * FROM silver.crm_prd_info;

SELECT * FROM bronze.crm_sales_details;

-- INSERT INTO SILVER TABLES
SELECT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
SELECT '>> Inserting Data Into: silver.crm_sales_details';
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_quantity,
    sls_price,
    sls_sales
)
SELECT sls_ord_num,
	   sls_prd_key,
       sls_cust_id,
       CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
			ELSE CAST(sls_order_dt AS DATE)
		END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(sls_ship_dt AS DATE)
		END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
			ELSE CAST(sls_due_dt AS DATE)
		END AS sls_due_dt,
       sls_quantity,
       CASE WHEN sls_price IS NULL OR sls_price <= 0
			 THEN CAST(ABS(sls_sales) / NULLIF(sls_quantity, 0) AS SIGNED)
             ELSE sls_price
		END AS sls_price,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity
			THEN ABS(sls_price) * sls_quantity
            ELSE sls_sales
		END AS sls_sales
FROM bronze.crm_sales_details;

-- Check for NULLS in date columns
SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
	  OR LENGTH(sls_due_dt) != 8
      OR sls_due_dt > 20500101
      OR sls_due_dt < 19000101;

-- Check for invalid date orders  
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Checking for sales, quantity and price columns
SELECT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity
	  OR sls_sales IS NULL
      OR sls_quantity IS NULL
      OR sls_price IS NULL
      OR sls_sales <= 0
      OR sls_quantity <= 0
      OR sls_price <= 0
ORDER BY sls_price, sls_quantity;

SELECT * FROM silver.crm_sales_details
ORDER BY sls_quantity DESC;

SELECT * FROM bronze.erp_cust_az12;

SELECT * FROM silver.crm_cust_info;

-- INSERT INTO SILVER TABLES
SELECT '>> Truncating Table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
SELECT '>> Inserting Data Into: silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12(
	cid,
    bdate,
    gen
)
SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		 ELSE cid
	END AS cid,
    CASE WHEN bdate > CURRENT_DATE() THEN NULL
		 ELSE bdate
	END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;

-- Data Standardization and Consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;

SELECT * FROM silver.erp_loc_a101;

SELECT '>> Truncating Table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
SELECT '>> Inserting Data Into: silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101(
	cid,
    cntry
)
SELECT 
    REPLACE(cid, '-', ''),
    CASE WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
         ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;

-- Data Standardization and Consistency
SELECT DISTINCT cntry FROM silver.erp_loc_a101;


SELECT * FROM silver.erp_px_cat_g1v2;


-- INSERT INTO SILVER TABLES
SELECT '>> Truncating Table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.px_cat_g1v2;
SELECT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
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
    maintenance
FROM bronze.erp_px_cat_g1v2;

-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standardization and Consistency
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;
