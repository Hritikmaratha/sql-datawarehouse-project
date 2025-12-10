if OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
  DROP TABLE bronze.crm_cust_info;
create table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);

if OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
  DROP TABLE bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
);
if OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
  DROP TABLE bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);
if OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
  DROP TABLE bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50)
);

if OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
  DROP TABLE bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50)
);

if OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
  DROP TABLE bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
);







create or alter procedure bronze.load_bronze as
begin
     Declare @start_time DATETIME , @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time Datetime; 
     begin TRY
         set @batch_start_time = getdate();
         print '==================================';
         print 'Loading Bronze Layer';
         print '==================================';


         print '----------------------------------';
         print 'Loading CRM Tables';
         print '----------------------------------';
         set @start_time = GETDATE();
         print '>> Truncating Table: bronze.crm_cust_info';
         TRUNCATE TABLE bronze.crm_cust_info;

         print '>> Inserting Data into Table: bronze.crm_cust_info';

         BULK INSERT bronze.crm_cust_info
         from 'C:\Users\hriti\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
         with (
               firstrow = 2,
               FIELDTERMINATOR = ',',
               TABLOCK
              );
         set @end_time = GETDATE();
          print '>> Load Duration:'+ cast(datediff(second,@start_time,@end_time) as nvarchar)+ 'seconds';
          print '>> Truncating Table: bronze.crm_prd_info';
          TRUNCATE TABLE bronze.crm_prd_info;

          print '>> Inserting Data into Table: bronze.crm_prd_info';
          BULK INSERT bronze.crm_prd_info
          from 'C:\Users\hriti\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
          with (
               firstrow = 2,
               FIELDTERMINATOR = ',',
               TABLOCK
               );

          print '>> Truncating Table: bronze.crm_sales_details';
          TRUNCATE TABLE bronze.crm_sales_details;

          print '>> Inserting Data into Table: bronze.crm_sales_details';

          BULK INSERT bronze.crm_sales_details
          from 'C:\Users\hriti\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
          with (
                firstrow = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );


          print '----------------------------------'
          print 'Loading ERP Tables'
          print '----------------------------------'

          print '>> Truncating Table: bronze.erp_cust_az12';
          TRUNCATE TABLE bronze.erp_cust_az12;

          print '>> Inserting Data into Table: bronze.erp_cust_az12';
          BULK INSERT bronze.erp_cust_az12
          from 'C:\Users\hriti\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
          with (
                firstrow = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
               );

          print '>> Truncating Table: bronze.erp_loc_a101';
          TRUNCATE TABLE bronze.erp_loc_a101;

          print '>> Inserting Data into Table: bronze.erp_loc_a101';
          BULK INSERT bronze.erp_loc_a101
          from 'C:\Users\hriti\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
          with (
                firstrow = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
               );

          print '>> Truncating Table: erp_px_cat_g1v2';
          TRUNCATE TABLE bronze.erp_px_cat_g1v2;

          print '>> Inserting Data into Table: bronze.erp_px_cat_g1v2';
          BULK INSERT bronze.erp_px_cat_g1v2
          from 'C:\Users\hriti\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
          with (
                firstrow = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
               );
          set @batch_end_time = getdate();
          print '>> Load Duration:'+ cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar)+ 'seconds';
end TRY
Begin Catch 
     print '==================================';
     print 'Error Occured during loading Bronze Layer'
     print 'Error Message' + ERROR_MESSAGE();
     print 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
     print 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
     print '==================================';
end Catch
end
