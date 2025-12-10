
============================================
Store Procedure : To load Bronze Layer (Source --> Bronze)
Use :
    exec bronze.load_bronze
============================================


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
