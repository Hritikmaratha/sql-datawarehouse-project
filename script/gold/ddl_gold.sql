CREATE VIEW gold.dim_customers as 
select 
     row_number () over (order by cst_id) as customer_key,
     ci.cst_id as customer_id,
     ci.cst_key as customer_number,
     ci.cst_firstname as first_name,
     ci.cst_lastname as last_name,
     la.cntry as country,
     ci.cst_material_status as martial_status,
     case when ci.cst_gndr != 'N/A' then ci.cst_gndr
          else coalesce(ca.gen,'N/A') 
     END AS gender,
     ca.bdate as birthdate,
     ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ca.cid = ci.cst_key
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid;


select distinct ci.cst_gndr,ca.gen, 
case when ci.cst_gndr != 'N/A' then ci.cst_gndr
else coalesce(ca.gen,'N/A') 
END AS new_gen
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ca.cid = ci.cst_key
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid;

SELECT distinct(gender) FROM gold.dim_customers;


CREATE VIEW gold.dim_products AS
select
row_number() over(order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as price,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where pn.prd_end_dt is null;

SELECT * FROM gold.dim_products;


create view gold.factsales as
SELECT 
sd.sls_ord_num as oder_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales,
sd.sls_quantity as quantity,
sd.sls_price as price
FROM silver.crm_sales_details as sd
left join gold.dim_products as pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers as cu
on cu.customer_id = sd.sls_cust_id;

select * from gold.factsales as f
left join gold.dim_customers as c
on c.customer_key = f.customer_key
where c.customer_key is null;
