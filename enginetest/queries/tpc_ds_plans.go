package queries

var TpcdsPlanTests = []QueryPlanTest{
	{
		Query: `
-- start query 1 in stream 0 using template query1.tpl
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select  c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'TN'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100;`,
	},
	{
		Query: `
		-- start query 2 in stream 0 using template query2.tpl
		with wscs as
	(select sold_date_sk
		,sales_price
		from (select ws_sold_date_sk sold_date_sk
		,ws_ext_sales_price sales_price
		from web_sales
		union all
		select cs_sold_date_sk sold_date_sk
		,cs_ext_sales_price sales_price
		from catalog_sales) dt),
		wswscs as
	(select d_week_seq,
		sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
		sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
		sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
		sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
		sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
		sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
		sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
		from wscs
		,date_dim
		where d_date_sk = sold_date_sk
		group by d_week_seq)
		select d_week_seq1
		,round(sun_sales1/sun_sales2,2)
		,round(mon_sales1/mon_sales2,2)
		,round(tue_sales1/tue_sales2,2)
		,round(wed_sales1/wed_sales2,2)
		,round(thu_sales1/thu_sales2,2)
		,round(fri_sales1/fri_sales2,2)
		,round(sat_sales1/sat_sales2,2)
		from
	(select wswscs.d_week_seq d_week_seq1
		,sun_sales sun_sales1
		,mon_sales mon_sales1
		,tue_sales tue_sales1
		,wed_sales wed_sales1
		,thu_sales thu_sales1
		,fri_sales fri_sales1
		,sat_sales sat_sales1
		from wswscs,date_dim
		where date_dim.d_week_seq = wswscs.d_week_seq and
		d_year = 1998) y,
	(select wswscs.d_week_seq d_week_seq2
		,sun_sales sun_sales2
		,mon_sales mon_sales2
		,tue_sales tue_sales2
		,wed_sales wed_sales2
		,thu_sales thu_sales2
		,fri_sales fri_sales2
		,sat_sales sat_sales2
		from wswscs
		,date_dim
		where date_dim.d_week_seq = wswscs.d_week_seq and
		d_year = 1998+1) z
		where d_week_seq1=d_week_seq2-53
		order by d_week_seq1;`,
	},
	{
		Query: `
-- start query 3 in stream 0 using template query3.tpl
select  dt.d_year 
       ,item.i_brand_id brand_id 
       ,item.i_brand brand
       ,sum(ss_sales_price) sum_agg
 from  date_dim dt 
      ,store_sales
      ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = 816
   and dt.d_moy=11
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 limit 100;`,
	},
	{
		Query: `
		-- start query 4 in stream 0 using template query4.tpl
		with year_total as (
		select c_customer_id customer_id
		,c_first_name customer_first_name
		,c_last_name customer_last_name
		,c_preferred_cust_flag customer_preferred_cust_flag
		,c_birth_country customer_birth_country
		,c_login customer_login
		,c_email_address customer_email_address
		,d_year dyear
		,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
		,'s' sale_type
		from customer
		,store_sales
		,date_dim
		where c_customer_sk = ss_customer_sk
		and ss_sold_date_sk = d_date_sk
		group by c_customer_id
		,c_first_name
		,c_last_name
		,c_preferred_cust_flag
		,c_birth_country
		,c_login
		,c_email_address
		,d_year
		union all
		select c_customer_id customer_id
		,c_first_name customer_first_name
		,c_last_name customer_last_name
		,c_preferred_cust_flag customer_preferred_cust_flag
		,c_birth_country customer_birth_country
		,c_login customer_login
		,c_email_address customer_email_address
		,d_year dyear
		,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total
		,'c' sale_type
		from customer
		,catalog_sales
		,date_dim
		where c_customer_sk = cs_bill_customer_sk
		and cs_sold_date_sk = d_date_sk
		group by c_customer_id
		,c_first_name
		,c_last_name
		,c_preferred_cust_flag
		,c_birth_country
		,c_login
		,c_email_address
		,d_year
		union all
		select c_customer_id customer_id
		,c_first_name customer_first_name
		,c_last_name customer_last_name
		,c_preferred_cust_flag customer_preferred_cust_flag
		,c_birth_country customer_birth_country
		,c_login customer_login
		,c_email_address customer_email_address
		,d_year dyear
		,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total
		,'w' sale_type
		from customer
		,web_sales
		,date_dim
		where c_customer_sk = ws_bill_customer_sk
		and ws_sold_date_sk = d_date_sk
		group by c_customer_id
		,c_first_name
		,c_last_name
		,c_preferred_cust_flag
		,c_birth_country
		,c_login
		,c_email_address
		,d_year
	)
		select
		t_s_secyear.customer_id
		,t_s_secyear.customer_first_name
		,t_s_secyear.customer_last_name
		,t_s_secyear.customer_birth_country
		from year_total t_s_firstyear
		,year_total t_s_secyear
		,year_total t_c_firstyear
		,year_total t_c_secyear
		,year_total t_w_firstyear
		,year_total t_w_secyear
		where t_s_secyear.customer_id = t_s_firstyear.customer_id
		and t_s_firstyear.customer_id = t_c_secyear.customer_id
		and t_s_firstyear.customer_id = t_c_firstyear.customer_id
		and t_s_firstyear.customer_id = t_w_firstyear.customer_id
		and t_s_firstyear.customer_id = t_w_secyear.customer_id
		and t_s_firstyear.sale_type = 's'
		and t_c_firstyear.sale_type = 'c'
		and t_w_firstyear.sale_type = 'w'
		and t_s_secyear.sale_type = 's'
		and t_c_secyear.sale_type = 'c'
		and t_w_secyear.sale_type = 'w'
		and t_s_firstyear.dyear =  1999
		and t_s_secyear.dyear = 1999+1
		and t_c_firstyear.dyear =  1999
		and t_c_secyear.dyear =  1999+1
		and t_w_firstyear.dyear = 1999
		and t_w_secyear.dyear = 1999+1
		and t_s_firstyear.year_total > 0
		and t_c_firstyear.year_total > 0
		and t_w_firstyear.year_total > 0
		and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
		> case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
		and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
		> case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
		order by t_s_secyear.customer_id
		,t_s_secyear.customer_first_name
		,t_s_secyear.customer_last_name
		,t_s_secyear.customer_birth_country
		limit 100;`,
	},
	{
		Query: `
-- start query 5 in stream 0 using template query5.tpl
with ssr as
 (select s_store_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as returns,
        sum(net_loss) as profit_loss
 from
  ( select  ss_store_sk as store_sk,
            ss_sold_date_sk  as date_sk,
            ss_ext_sales_price as sales_price,
            ss_net_profit as profit,
            cast(0 as decimal(7,2)) as return_amt,
            cast(0 as decimal(7,2)) as net_loss
    from store_sales
    union all
    select sr_store_sk as store_sk,
           sr_returned_date_sk as date_sk,
           cast(0 as decimal(7,2)) as sales_price,
           cast(0 as decimal(7,2)) as profit,
           sr_return_amt as return_amt,
           sr_net_loss as net_loss
    from store_returns
   ) salesreturns,
     date_dim,
     store
 where date_sk = d_date_sk
       and d_date between cast('2000-08-19' as date) 
                  and (cast('2000-08-19' as date) +  interval 14 day)
       and store_sk = s_store_sk
 group by s_store_id)
 ,
 csr as
 (select cp_catalog_page_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as returns,
        sum(net_loss) as profit_loss
 from
  ( select  cs_catalog_page_sk as page_sk,
            cs_sold_date_sk  as date_sk,
            cs_ext_sales_price as sales_price,
            cs_net_profit as profit,
            cast(0 as decimal(7,2)) as return_amt,
            cast(0 as decimal(7,2)) as net_loss
    from catalog_sales
    union all
    select cr_catalog_page_sk as page_sk,
           cr_returned_date_sk as date_sk,
           cast(0 as decimal(7,2)) as sales_price,
           cast(0 as decimal(7,2)) as profit,
           cr_return_amount as return_amt,
           cr_net_loss as net_loss
    from catalog_returns
   ) salesreturns,
     date_dim,
     catalog_page
 where date_sk = d_date_sk
       and d_date between cast('2000-08-19' as date)
                  and (cast('2000-08-19' as date) +  interval 14 day)
       and page_sk = cp_catalog_page_sk
 group by cp_catalog_page_id)
 ,
 wsr as
 (select web_site_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as returns,
        sum(net_loss) as profit_loss
 from
  ( select  ws_web_site_sk as wsr_web_site_sk,
            ws_sold_date_sk  as date_sk,
            ws_ext_sales_price as sales_price,
            ws_net_profit as profit,
            cast(0 as decimal(7,2)) as return_amt,
            cast(0 as decimal(7,2)) as net_loss
    from web_sales
    union all
    select ws_web_site_sk as wsr_web_site_sk,
           wr_returned_date_sk as date_sk,
           cast(0 as decimal(7,2)) as sales_price,
           cast(0 as decimal(7,2)) as profit,
           wr_return_amt as return_amt,
           wr_net_loss as net_loss
    from web_returns left outer join web_sales on
         ( wr_item_sk = ws_item_sk
           and wr_order_number = ws_order_number)
   ) salesreturns,
     date_dim,
     web_site
 where date_sk = d_date_sk
       and d_date between cast('2000-08-19' as date)
                  and (cast('2000-08-19' as date) +  interval 14 day)
       and wsr_web_site_sk = web_site_sk
 group by web_site_id)
  select  channel
        , id
        , sum(sales) as sales
        , sum(returns) as returns
        , sum(profit) as profit
 from 
 (select 'store channel' as channel
        , 'store' || s_store_id as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from   ssr
 union all
 select 'catalog channel' as channel
        , 'catalog_page' || cp_catalog_page_id as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from  csr
 union all
 select 'web channel' as channel
        , 'web_site' || web_site_id as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from   wsr
 ) x
 group by rollup (channel, id)
 order by channel
         ,id
 limit 100;`,
		Skip: true,
	},
	{
		Query: `
		-- start query 6 in stream 0 using template query6.tpl
		select  a.ca_state state, count(*) cnt
		from customer_address a
		,customer c
		,store_sales s
		,date_dim d
		,item i
		where       a.ca_address_sk = c.c_current_addr_sk
		and c.c_customer_sk = s.ss_customer_sk
		and s.ss_sold_date_sk = d.d_date_sk
		and s.ss_item_sk = i.i_item_sk
		and d.d_month_seq =
	(select distinct (d_month_seq)
		from date_dim
		where d_year = 2002
		and d_moy = 3 )
		and i.i_current_price > 1.2 *
	(select avg(j.i_current_price)
		from item j
		where j.i_category = i.i_category)
		group by a.ca_state
		having count(*) >= 10
		order by cnt, a.ca_state
		limit 100;`,
	},
	{
		Query: `
-- start query 7 in stream 0 using template query7.tpl
select  i_item_id, 
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4 
 from store_sales, customer_demographics, date_dim, item, promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'F' and 
       cd_marital_status = 'W' and
       cd_education_status = 'College' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2001 
 group by i_item_id
 order by i_item_id
 limit 100;`,
	},
	{
		Query: `
		-- start query 8 in stream 0 using template query8.tpl
		select  s_store_name
		,sum(ss_net_profit)
		from store_sales
		,date_dim
		,store,
	(select ca_zip
		from (
		SELECT substr(ca_zip,1,5) ca_zip
		FROM customer_address
		WHERE substr(ca_zip,1,5) IN (
		'47602','16704','35863','28577','83910','36201',
		'58412','48162','28055','41419','80332',
		'38607','77817','24891','16226','18410',
		'21231','59345','13918','51089','20317',
		'17167','54585','67881','78366','47770',
		'18360','51717','73108','14440','21800',
		'89338','45859','65501','34948','25973',
		'73219','25333','17291','10374','18829',
		'60736','82620','41351','52094','19326',
		'25214','54207','40936','21814','79077',
		'25178','75742','77454','30621','89193',
		'27369','41232','48567','83041','71948',
		'37119','68341','14073','16891','62878',
		'49130','19833','24286','27700','40979',
		'50412','81504','94835','84844','71954',
		'39503','57649','18434','24987','12350',
		'86379','27413','44529','98569','16515',
		'27287','24255','21094','16005','56436',
		'91110','68293','56455','54558','10298',
		'83647','32754','27052','51766','19444',
		'13869','45645','94791','57631','20712',
		'37788','41807','46507','21727','71836',
		'81070','50632','88086','63991','20244',
		'31655','51782','29818','63792','68605',
		'94898','36430','57025','20601','82080',
		'33869','22728','35834','29086','92645',
		'98584','98072','11652','78093','57553',
		'43830','71144','53565','18700','90209',
		'71256','38353','54364','28571','96560',
		'57839','56355','50679','45266','84680',
		'34306','34972','48530','30106','15371',
		'92380','84247','92292','68852','13338',
		'34594','82602','70073','98069','85066',
		'47289','11686','98862','26217','47529',
		'63294','51793','35926','24227','14196',
		'24594','32489','99060','49472','43432',
		'49211','14312','88137','47369','56877',
		'20534','81755','15794','12318','21060',
		'73134','41255','63073','81003','73873',
		'66057','51184','51195','45676','92696',
		'70450','90669','98338','25264','38919',
		'59226','58581','60298','17895','19489',
		'52301','80846','95464','68770','51634',
		'19988','18367','18421','11618','67975',
		'25494','41352','95430','15734','62585',
		'97173','33773','10425','75675','53535',
		'17879','41967','12197','67998','79658',
		'59130','72592','14851','43933','68101',
		'50636','25717','71286','24660','58058',
		'72991','95042','15543','33122','69280',
		'11912','59386','27642','65177','17672',
		'33467','64592','36335','54010','18767',
		'63193','42361','49254','33113','33159',
		'36479','59080','11855','81963','31016',
		'49140','29392','41836','32958','53163',
		'13844','73146','23952','65148','93498',
		'14530','46131','58454','13376','13378',
		'83986','12320','17193','59852','46081',
		'98533','52389','13086','68843','31013',
		'13261','60560','13443','45533','83583',
		'11489','58218','19753','22911','25115',
		'86709','27156','32669','13123','51933',
		'39214','41331','66943','14155','69998',
		'49101','70070','35076','14242','73021',
		'59494','15782','29752','37914','74686',
		'83086','34473','15751','81084','49230',
		'91894','60624','17819','28810','63180',
		'56224','39459','55233','75752','43639',
		'55349','86057','62361','50788','31830',
		'58062','18218','85761','60083','45484',
		'21204','90229','70041','41162','35390',
		'16364','39500','68908','26689','52868',
		'81335','40146','11340','61527','61794',
		'71997','30415','59004','29450','58117',
		'69952','33562','83833','27385','61860',
		'96435','48333','23065','32961','84919',
		'61997','99132','22815','56600','68730',
		'48017','95694','32919','88217','27116',
		'28239','58032','18884','16791','21343',
		'97462','18569','75660','15475')
		intersect
		select ca_zip
		from (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt
		FROM customer_address, customer
		WHERE ca_address_sk = c_current_addr_sk and
		c_preferred_cust_flag='Y'
		group by ca_zip
		having count(*) > 10)A1)A2) V1
		where ss_store_sk = s_store_sk
		and ss_sold_date_sk = d_date_sk
		and d_qoy = 2 and d_year = 1998
		and (substr(s_zip,1,2) = substr(V1.ca_zip,1,2))
		group by s_store_name
		order by s_store_name
		limit 100;`,
	},
	{
		Query: `
-- start query 9 in stream 0 using template query9.tpl
select case when (select count(*) 
                  from store_sales 
                  where ss_quantity between 1 and 20) > 1071
            then (select avg(ss_ext_tax) 
                  from store_sales 
                  where ss_quantity between 1 and 20) 
            else (select avg(ss_net_paid_inc_tax)
                  from store_sales
                  where ss_quantity between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 21 and 40) > 39161
            then (select avg(ss_ext_tax)
                  from store_sales
                  where ss_quantity between 21 and 40) 
            else (select avg(ss_net_paid_inc_tax)
                  from store_sales
                  where ss_quantity between 21 and 40) end bucket2,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 41 and 60) > 29434
            then (select avg(ss_ext_tax)
                  from store_sales
                  where ss_quantity between 41 and 60)
            else (select avg(ss_net_paid_inc_tax)
                  from store_sales
                  where ss_quantity between 41 and 60) end bucket3,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 61 and 80) > 6568
            then (select avg(ss_ext_tax)
                  from store_sales
                  where ss_quantity between 61 and 80)
            else (select avg(ss_net_paid_inc_tax)
                  from store_sales
                  where ss_quantity between 61 and 80) end bucket4,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 81 and 100) > 21216
            then (select avg(ss_ext_tax)
                  from store_sales
                  where ss_quantity between 81 and 100)
            else (select avg(ss_net_paid_inc_tax)
                  from store_sales
                  where ss_quantity between 81 and 100) end bucket5
from reason
where r_reason_sk = 1
;`,
	},
	{
		Query: `
		-- start query 10 in stream 0 using template query10.tpl
		select
		cd_gender,
		cd_marital_status,
		cd_education_status,
		count(*) cnt1,
		cd_purchase_estimate,
		count(*) cnt2,
		cd_credit_rating,
		count(*) cnt3,
		cd_dep_count,
		count(*) cnt4,
		cd_dep_employed_count,
		count(*) cnt5,
		cd_dep_college_count,
		count(*) cnt6
		from
		customer c,customer_address ca,customer_demographics
		where
		c.c_current_addr_sk = ca.ca_address_sk and
		ca_county in ('Fairfield County','Campbell County','Washtenaw County','Escambia County','Cleburne County') and
		cd_demo_sk = c.c_current_cdemo_sk and
		exists (select *
		from store_sales,date_dim
		where c.c_customer_sk = ss_customer_sk and
		ss_sold_date_sk = d_date_sk and
		d_year = 2001 and
		d_moy between 3 and 3+3) and
	(exists (select *
		from web_sales,date_dim
		where c.c_customer_sk = ws_bill_customer_sk and
		ws_sold_date_sk = d_date_sk and
		d_year = 2001 and
		d_moy between 3 ANd 3+3) or
		exists (select *
		from catalog_sales,date_dim
		where c.c_customer_sk = cs_ship_customer_sk and
		cs_sold_date_sk = d_date_sk and
		d_year = 2001 and
		d_moy between 3 and 3+3))
		group by cd_gender,
		cd_marital_status,
		cd_education_status,
		cd_purchase_estimate,
		cd_credit_rating,
		cd_dep_count,
		cd_dep_employed_count,
		cd_dep_college_count
		order by cd_gender,
		cd_marital_status,
		cd_education_status,
		cd_purchase_estimate,
		cd_credit_rating,
		cd_dep_count,
		cd_dep_employed_count,
		cd_dep_college_count
		limit 100;`,
	},
	{
		Query: `
-- start query 11 in stream 0 using template query11.tpl
with year_total as (
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total
       ,'s' sale_type
 from customer
     ,store_sales
     ,date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag 
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year 
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(ws_ext_list_price-ws_ext_discount_amt) year_total
       ,'w' sale_type
 from customer
     ,web_sales
     ,date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag 
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
         )
  select  
                  t_s_secyear.customer_id
                 ,t_s_secyear.customer_first_name
                 ,t_s_secyear.customer_last_name
                 ,t_s_secyear.customer_email_address
 from year_total t_s_firstyear
     ,year_total t_s_secyear
     ,year_total t_w_firstyear
     ,year_total t_w_secyear
 where t_s_secyear.customer_id = t_s_firstyear.customer_id
         and t_s_firstyear.customer_id = t_w_secyear.customer_id
         and t_s_firstyear.customer_id = t_w_firstyear.customer_id
         and t_s_firstyear.sale_type = 's'
         and t_w_firstyear.sale_type = 'w'
         and t_s_secyear.sale_type = 's'
         and t_w_secyear.sale_type = 'w'
         and t_s_firstyear.dyear = 1998
         and t_s_secyear.dyear = 1998+1
         and t_w_firstyear.dyear = 1998
         and t_w_secyear.dyear = 1998+1
         and t_s_firstyear.year_total > 0
         and t_w_firstyear.year_total > 0
         and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else 0.0 end
             > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else 0.0 end
 order by t_s_secyear.customer_id
         ,t_s_secyear.customer_first_name
         ,t_s_secyear.customer_last_name
         ,t_s_secyear.customer_email_address
limit 100;`,
	},
	{
		Skip: true,
		Query: `
		-- start query 12 in stream 0 using template query12.tpl
		select  i_item_id
		,i_item_desc
		,i_category
		,i_class
		,i_current_price
		,sum(ws_ext_sales_price) as itemrevenue
		,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
	(partition by i_class) as revenueratio
		from
		web_sales
		,item
		,date_dim
		where
		ws_item_sk = i_item_sk
		and i_category in ('Men', 'Books', 'Electronics')
		and ws_sold_date_sk = d_date_sk
		and d_date between cast('2001-06-15' as date)
		and (cast('2001-06-15' as date) + interval 30 day)
		group by
		i_item_id
		,i_item_desc
		,i_category
		,i_class
		,i_current_price
		order by
		i_category
		,i_class
		,i_item_id
		,i_item_desc
		,revenueratio
		limit 100;`,
	},
	{
		Query: `
-- start query 13 in stream 0 using template query13.tpl
select avg(ss_quantity)
       ,avg(ss_ext_sales_price)
       ,avg(ss_ext_wholesale_cost)
       ,sum(ss_ext_wholesale_cost)
 from store_sales
     ,store
     ,customer_demographics
     ,household_demographics
     ,customer_address
     ,date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 2001
 and((ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'M'
  and cd_education_status = 'College'
  and ss_sales_price between 100.00 and 150.00
  and hd_dep_count = 3   
     )or
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'D'
  and cd_education_status = 'Primary'
  and ss_sales_price between 50.00 and 100.00   
  and hd_dep_count = 1
     ) or 
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'W'
  and cd_education_status = '2 yr Degree'
  and ss_sales_price between 150.00 and 200.00 
  and hd_dep_count = 1  
     ))
 and((ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('IL', 'TN', 'TX')
  and ss_net_profit between 100 and 200  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('WY', 'OH', 'ID')
  and ss_net_profit between 150 and 300  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('MS', 'SC', 'IA')
  and ss_net_profit between 50 and 250  
     ))
;`,
	},
	{
		Query: `
		-- start query 14 in stream 0 using template query14.tpl
		with  cross_items as
	(select i_item_sk ss_item_sk
		from item,
	(select iss.i_brand_id brand_id
		,iss.i_class_id class_id
		,iss.i_category_id category_id
		from store_sales
		,item iss
		,date_dim d1
		where ss_item_sk = iss.i_item_sk
		and ss_sold_date_sk = d1.d_date_sk
		and d1.d_year between 1999 AND 1999 + 2
		intersect
		select ics.i_brand_id
		,ics.i_class_id
		,ics.i_category_id
		from catalog_sales
		,item ics
		,date_dim d2
		where cs_item_sk = ics.i_item_sk
		and cs_sold_date_sk = d2.d_date_sk
		and d2.d_year between 1999 AND 1999 + 2
		intersect
		select iws.i_brand_id
		,iws.i_class_id
		,iws.i_category_id
		from web_sales
		,item iws
		,date_dim d3
		where ws_item_sk = iws.i_item_sk
		and ws_sold_date_sk = d3.d_date_sk
		and d3.d_year between 1999 AND 1999 + 2)
		where i_brand_id = brand_id
		and i_class_id = class_id
		and i_category_id = category_id
	),
		avg_sales as
	(select avg(quantity*list_price) average_sales
		from (select ss_quantity quantity
		,ss_list_price list_price
		from store_sales
		,date_dim
		where ss_sold_date_sk = d_date_sk
		and d_year between 1999 and 1999 + 2
		union all
		select cs_quantity quantity
		,cs_list_price list_price
		from catalog_sales
		,date_dim
		where cs_sold_date_sk = d_date_sk
		and d_year between 1999 and 1999 + 2
		union all
		select ws_quantity quantity
		,ws_list_price list_price
		from web_sales
		,date_dim
		where ws_sold_date_sk = d_date_sk
		and d_year between 1999 and 1999 + 2) x)
		select  channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
		from(
		select 'store' channel, i_brand_id,i_class_id
		,i_category_id,sum(ss_quantity*ss_list_price) sales
		, count(*) number_sales
		from store_sales
		,item
		,date_dim
		where ss_item_sk in (select ss_item_sk from cross_items)
		and ss_item_sk = i_item_sk
		and ss_sold_date_sk = d_date_sk
		and d_year = 1999+2
		and d_moy = 11
		group by i_brand_id,i_class_id,i_category_id
		having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)
		union all
		select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
		from catalog_sales
		,item
		,date_dim
		where cs_item_sk in (select ss_item_sk from cross_items)
		and cs_item_sk = i_item_sk
		and cs_sold_date_sk = d_date_sk
		and d_year = 1999+2
		and d_moy = 11
		group by i_brand_id,i_class_id,i_category_id
		having sum(cs_quantity*cs_list_price) > (select average_sales from avg_sales)
		union all
		select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
		from web_sales
		,item
		,date_dim
		where ws_item_sk in (select ss_item_sk from cross_items)
		and ws_item_sk = i_item_sk
		and ws_sold_date_sk = d_date_sk
		and d_year = 1999+2
		and d_moy = 11
		group by i_brand_id,i_class_id,i_category_id
		having sum(ws_quantity*ws_list_price) > (select average_sales from avg_sales)
	) y
		group by rollup (channel, i_brand_id,i_class_id,i_category_id)
		order by channel,i_brand_id,i_class_id,i_category_id
		limit 100;
		with  cross_items as
	(select i_item_sk ss_item_sk
		from item,
	(select iss.i_brand_id brand_id
		,iss.i_class_id class_id
		,iss.i_category_id category_id
		from store_sales
		,item iss
		,date_dim d1
		where ss_item_sk = iss.i_item_sk
		and ss_sold_date_sk = d1.d_date_sk
		and d1.d_year between 1999 AND 1999 + 2
		intersect
		select ics.i_brand_id
		,ics.i_class_id
		,ics.i_category_id
		from catalog_sales
		,item ics
		,date_dim d2
		where cs_item_sk = ics.i_item_sk
		and cs_sold_date_sk = d2.d_date_sk
		and d2.d_year between 1999 AND 1999 + 2
		intersect
		select iws.i_brand_id
		,iws.i_class_id
		,iws.i_category_id
		from web_sales
		,item iws
		,date_dim d3
		where ws_item_sk = iws.i_item_sk
		and ws_sold_date_sk = d3.d_date_sk
		and d3.d_year between 1999 AND 1999 + 2) x
		where i_brand_id = brand_id
		and i_class_id = class_id
		and i_category_id = category_id
	),
		avg_sales as
	(select avg(quantity*list_price) average_sales
		from (select ss_quantity quantity
		,ss_list_price list_price
		from store_sales
		,date_dim
		where ss_sold_date_sk = d_date_sk
		and d_year between 1999 and 1999 + 2
		union all
		select cs_quantity quantity
		,cs_list_price list_price
		from catalog_sales
		,date_dim
		where cs_sold_date_sk = d_date_sk
		and d_year between 1999 and 1999 + 2
		union all
		select ws_quantity quantity
		,ws_list_price list_price
		from web_sales
		,date_dim
		where ws_sold_date_sk = d_date_sk
		and d_year between 1999 and 1999 + 2) x)
		select  this_year.channel ty_channel
		,this_year.i_brand_id ty_brand
		,this_year.i_class_id ty_class
		,this_year.i_category_id ty_category
		,this_year.sales ty_sales
		,this_year.number_sales ty_number_sales
		,last_year.channel ly_channel
		,last_year.i_brand_id ly_brand
		,last_year.i_class_id ly_class
		,last_year.i_category_id ly_category
		,last_year.sales ly_sales
		,last_year.number_sales ly_number_sales
		from
	(select 'store' channel, i_brand_id,i_class_id,i_category_id
		,sum(ss_quantity*ss_list_price) sales, count(*) number_sales
		from store_sales
		,item
		,date_dim
		where ss_item_sk in (select ss_item_sk from cross_items)
		and ss_item_sk = i_item_sk
		and ss_sold_date_sk = d_date_sk
		and d_week_seq = (select d_week_seq
		from date_dim
		where d_year = 1999 + 1
		and d_moy = 12
		and d_dom = 3)
		group by i_brand_id,i_class_id,i_category_id
		having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)) this_year,
	(select 'store' channel, i_brand_id,i_class_id
		,i_category_id, sum(ss_quantity*ss_list_price) sales, count(*) number_sales
		from store_sales
		,item
		,date_dim
		where ss_item_sk in (select ss_item_sk from cross_items)
		and ss_item_sk = i_item_sk
		and ss_sold_date_sk = d_date_sk
		and d_week_seq = (select d_week_seq
		from date_dim
		where d_year = 1999
		and d_moy = 12
		and d_dom = 3)
		group by i_brand_id,i_class_id,i_category_id
		having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)) last_year
		where this_year.i_brand_id= last_year.i_brand_id
		and this_year.i_class_id = last_year.i_class_id
		and this_year.i_category_id = last_year.i_category_id
		order by this_year.channel, this_year.i_brand_id, this_year.i_class_id, this_year.i_category_id
		limit 100;`,
		Skip: true,
	},
	{
		Query: `
-- start query 15 in stream 0 using template query15.tpl
select  ca_zip
       ,sum(cs_sales_price)
 from catalog_sales
     ,customer
     ,customer_address
     ,date_dim
 where cs_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA')
 	      or cs_sales_price > 500)
 	and cs_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2001
 group by ca_zip
 order by ca_zip
 limit 100;`,
	},
	{
		Query: `
		-- start query 16 in stream 0 using template query16.tpl
		select
		count(distinct cs_order_number) as "order count"
		,sum(cs_ext_ship_cost) as "total shipping cost"
		,sum(cs_net_profit) as "total net profit"
		from
		catalog_sales cs1
		,date_dim
		,customer_address
		,call_center
		where
		d_date between '2002-4-01' and
	(cast('2002-4-01' as date) + interval 60 day)
		and cs1.cs_ship_date_sk = d_date_sk
		and cs1.cs_ship_addr_sk = ca_address_sk
		and ca_state = 'PA'
		and cs1.cs_call_center_sk = cc_call_center_sk
		and cc_county in ('Williamson County','Williamson County','Williamson County','Williamson County',
		'Williamson County'
	)
		and exists (select *
		from catalog_sales cs2
		where cs1.cs_order_number = cs2.cs_order_number
		and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
		and not exists(select *
		from catalog_returns cr1
		where cs1.cs_order_number = cr1.cr_order_number)
		order by count(distinct cs_order_number)
		limit 100;`,
	},
	{
		Query: `
-- start query 17 in stream 0 using template query17.tpl
select  i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as store_returns_quantitycount
       ,avg(sr_return_quantity) as store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales
     ,store_returns
     ,catalog_sales
     ,date_dim d1
     ,date_dim d2
     ,date_dim d3
     ,store
     ,item
 where d1.d_quarter_name = '2001Q1'
   and d1.d_date_sk = ss_sold_date_sk
   and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk
   and ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
   and sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('2001Q1','2001Q2','2001Q3')
   and sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
   and cs_sold_date_sk = d3.d_date_sk
   and d3.d_quarter_name in ('2001Q1','2001Q2','2001Q3')
 group by i_item_id
         ,i_item_desc
         ,s_state
 order by i_item_id
         ,i_item_desc
         ,s_state
limit 100;`,
	},
	{
		Query: `
		-- start query 18 in stream 0 using template query18.tpl
		select  i_item_id,
		ca_country,
		ca_state,
		ca_county,
		avg( cast(cs_quantity as decimal(12,2))) agg1,
		avg( cast(cs_list_price as decimal(12,2))) agg2,
		avg( cast(cs_coupon_amt as decimal(12,2))) agg3,
		avg( cast(cs_sales_price as decimal(12,2))) agg4,
		avg( cast(cs_net_profit as decimal(12,2))) agg5,
		avg( cast(c_birth_year as decimal(12,2))) agg6,
		avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7
		from catalog_sales, customer_demographics cd1,
		customer_demographics cd2, customer, customer_address, date_dim, item
		where cs_sold_date_sk = d_date_sk and
		cs_item_sk = i_item_sk and
		cs_bill_cdemo_sk = cd1.cd_demo_sk and
		cs_bill_customer_sk = c_customer_sk and
		cd1.cd_gender = 'F' and
		cd1.cd_education_status = 'Primary' and
		c_current_cdemo_sk = cd2.cd_demo_sk and
		c_current_addr_sk = ca_address_sk and
		c_birth_month in (1,3,7,11,10,4) and
		d_year = 2001 and
		ca_state in ('AL','MO','TN'
		,'GA','MT','IN','CA')
		group by rollup (i_item_id, ca_country, ca_state, ca_county)
		order by ca_country,
		ca_state,
		ca_county,
		i_item_id
		limit 100;`,
		Skip: true,
	},
	{
		Query: `
-- start query 19 in stream 0 using template query19.tpl
select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item,customer,customer_address,store
 where d_date_sk = ss_sold_date_sk
   and ss_item_sk = i_item_sk
   and i_manager_id=14
   and d_moy=11
   and d_year=2002
   and ss_customer_sk = c_customer_sk 
   and c_current_addr_sk = ca_address_sk
   and substr(ca_zip,1,5) <> substr(s_zip,1,5) 
   and ss_store_sk = s_store_sk 
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
limit 100 ;`,
	},
	{
		Query: `
		-- start query 20 in stream 0 using template query20.tpl
		select  i_item_id
		,i_item_desc
		,i_category
		,i_class
		,i_current_price
		,sum(cs_ext_sales_price) as itemrevenue
		,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
	(partition by i_class) as revenueratio
		from	catalog_sales
		,item
		,date_dim
		where cs_item_sk = i_item_sk
		and i_category in ('Books', 'Music', 'Sports')
		and cs_sold_date_sk = d_date_sk
		and d_date between cast('2002-06-18' as date)
		and (cast('2002-06-18' as date) + interval 30 day)
		group by i_item_id
		,i_item_desc
		,i_category
		,i_class
		,i_current_price
		order by i_category
		,i_class
		,i_item_id
		,i_item_desc
		,revenueratio
		limit 100;`,
	},
	{
		Query: `
-- start query 21 in stream 0 using template query21.tpl
select  *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when (cast(d_date as date) < cast ('1999-06-22' as date))
	                then inv_quantity_on_hand 
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as date) >= cast ('1999-06-22' as date))
                      then inv_quantity_on_hand 
                      else 0 end) as inv_after
   from inventory
       ,warehouse
       ,item
       ,date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and d_date between (cast ('1999-06-22' as date) - interval 30 day)
                    and (cast ('1999-06-22' as date) + interval 30 day)
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0 
             then inv_after / inv_before 
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 limit 100;`,
	},
	{
		Query: `
		-- start query 22 in stream 0 using template query22.tpl
		select  i_product_name
		,i_brand
		,i_class
		,i_category
		,avg(inv_quantity_on_hand) qoh
		from inventory
		,date_dim
		,item
		where inv_date_sk=d_date_sk
		and inv_item_sk=i_item_sk
		and d_month_seq between 1200 and 1200 + 11
		group by rollup(i_product_name
		,i_brand
		,i_class
		,i_category)
		order by qoh, i_product_name, i_brand, i_class, i_category
		limit 100;`,
		Skip: true,
	},
	{
		Query: `
-- start query 23 in stream 0 using template query23.tpl
with frequent_ss_items as 
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim 
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk 
    and d_year in (2000,2000+1,2000+2,2000+3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax 
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3) 
        group by c_customer_sk)),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
from
 max_store_sales))
  select  sum(sales)
 from (select cs_quantity*cs_list_price sales
       from catalog_sales
           ,date_dim 
       where d_year = 2000 
         and d_moy = 7 
         and cs_sold_date_sk = d_date_sk 
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
      union all
      select ws_quantity*ws_list_price sales
       from web_sales 
           ,date_dim 
       where d_year = 2000 
         and d_moy = 7 
         and ws_sold_date_sk = d_date_sk 
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)) 
 limit 100;
with frequent_ss_items as
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk
    and d_year in (2000,2000 + 1,2000 + 2,2000 + 3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3)
        group by c_customer_sk)),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
 from max_store_sales))
  select  c_last_name,c_first_name,sales
 from (select c_last_name,c_first_name,sum(cs_quantity*cs_list_price) sales
        from catalog_sales
            ,customer
            ,date_dim 
        where d_year = 2000 
         and d_moy = 7 
         and cs_sold_date_sk = d_date_sk 
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and cs_bill_customer_sk = c_customer_sk 
       group by c_last_name,c_first_name
      union all
      select c_last_name,c_first_name,sum(ws_quantity*ws_list_price) sales
       from web_sales
           ,customer
           ,date_dim 
       where d_year = 2000 
         and d_moy = 7 
         and ws_sold_date_sk = d_date_sk 
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and ws_bill_customer_sk = c_customer_sk
       group by c_last_name,c_first_name) 
     order by c_last_name,c_first_name,sales
  limit 100;`,
	},
	{
		Query: `
		-- start query 24 in stream 0 using template query24.tpl
		with ssales as
	(select c_last_name
		,c_first_name
		,s_store_name
		,ca_state
		,s_state
		,i_color
		,i_current_price
		,i_manager_id
		,i_units
		,i_size
		,sum(ss_net_paid) netpaid
		from store_sales
		,store_returns
		,store
		,item
		,customer
		,customer_address
		where ss_ticket_number = sr_ticket_number
		and ss_item_sk = sr_item_sk
		and ss_customer_sk = c_customer_sk
		and ss_item_sk = i_item_sk
		and ss_store_sk = s_store_sk
		and c_current_addr_sk = ca_address_sk
		and c_birth_country <> upper(ca_country)
		and s_zip = ca_zip
		and s_market_id=5
		group by c_last_name
		,c_first_name
		,s_store_name
		,ca_state
		,s_state
		,i_color
		,i_current_price
		,i_manager_id
		,i_units
		,i_size)
		select c_last_name
		,c_first_name
		,s_store_name
		,sum(netpaid) paid
		from ssales
		where i_color = 'aquamarine'
		group by c_last_name
		,c_first_name
		,s_store_name
		having sum(netpaid) > (select 0.05*avg(netpaid)
		from ssales)
		order by c_last_name
		,c_first_name
		,s_store_name
		;
		with ssales as
	(select c_last_name
		,c_first_name
		,s_store_name
		,ca_state
		,s_state
		,i_color
		,i_current_price
		,i_manager_id
		,i_units
		,i_size
		,sum(ss_net_paid) netpaid
		from store_sales
		,store_returns
		,store
		,item
		,customer
		,customer_address
		where ss_ticket_number = sr_ticket_number
		and ss_item_sk = sr_item_sk
		and ss_customer_sk = c_customer_sk
		and ss_item_sk = i_item_sk
		and ss_store_sk = s_store_sk
		and c_current_addr_sk = ca_address_sk
		and c_birth_country <> upper(ca_country)
		and s_zip = ca_zip
		and s_market_id = 5
		group by c_last_name
		,c_first_name
		,s_store_name
		,ca_state
		,s_state
		,i_color
		,i_current_price
		,i_manager_id
		,i_units
		,i_size)
		select c_last_name
		,c_first_name
		,s_store_name
		,sum(netpaid) paid
		from ssales
		where i_color = 'seashell'
		group by c_last_name
		,c_first_name
		,s_store_name
		having sum(netpaid) > (select 0.05*avg(netpaid)
		from ssales)
		order by c_last_name
		,c_first_name
		,s_store_name
		;`,
	},
	{
		Query: `
-- start query 25 in stream 0 using template query25.tpl
select  
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 ,max(ss_net_profit) as store_sales_profit
 ,max(sr_net_loss) as store_returns_loss
 ,max(cs_net_profit) as catalog_sales_profit
 from
 store_sales
 ,store_returns
 ,catalog_sales
 ,date_dim d1
 ,date_dim d2
 ,date_dim d3
 ,store
 ,item
 where
 d1.d_moy = 4
 and d1.d_year = 1999
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk = ss_item_sk
 and s_store_sk = ss_store_sk
 and ss_customer_sk = sr_customer_sk
 and ss_item_sk = sr_item_sk
 and ss_ticket_number = sr_ticket_number
 and sr_returned_date_sk = d2.d_date_sk
 and d2.d_moy               between 4 and  10
 and d2.d_year              = 1999
 and sr_customer_sk = cs_bill_customer_sk
 and sr_item_sk = cs_item_sk
 and cs_sold_date_sk = d3.d_date_sk
 and d3.d_moy               between 4 and  10 
 and d3.d_year              = 1999
 group by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 order by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 limit 100;`,
	},
	{
		Query: `
		-- start query 26 in stream 0 using template query26.tpl
		select  i_item_id,
		avg(cs_quantity) agg1,
		avg(cs_list_price) agg2,
		avg(cs_coupon_amt) agg3,
		avg(cs_sales_price) agg4
		from catalog_sales, customer_demographics, date_dim, item, promotion
		where cs_sold_date_sk = d_date_sk and
		cs_item_sk = i_item_sk and
		cs_bill_cdemo_sk = cd_demo_sk and
		cs_promo_sk = p_promo_sk and
		cd_gender = 'M' and
		cd_marital_status = 'W' and
		cd_education_status = 'Unknown' and
	(p_channel_email = 'N' or p_channel_event = 'N') and
		d_year = 2002
		group by i_item_id
		order by i_item_id
		limit 100;`,
	},
	{
		Query: `
-- start query 27 in stream 0 using template query27.tpl
select  i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'M' and
       cd_marital_status = 'W' and
       cd_education_status = 'Secondary' and
       d_year = 1999 and
       s_state in ('TN','TN', 'TN', 'TN', 'TN', 'TN')
 group by rollup (i_item_id, s_state)
 order by i_item_id
         ,s_state
 limit 100;`,
		Skip: true,
	},
	{
		Query: `
		-- start query 28 in stream 0 using template query28.tpl
		select  *
		from (select avg(ss_list_price) B1_LP
		,count(ss_list_price) B1_CNT
		,count(distinct ss_list_price) B1_CNTD
		from store_sales
		where ss_quantity between 0 and 5
		and (ss_list_price between 107 and 107+10
		or ss_coupon_amt between 1319 and 1319+1000
		or ss_wholesale_cost between 60 and 60+20)) B1,
	(select avg(ss_list_price) B2_LP
		,count(ss_list_price) B2_CNT
		,count(distinct ss_list_price) B2_CNTD
		from store_sales
		where ss_quantity between 6 and 10
		and (ss_list_price between 23 and 23+10
		or ss_coupon_amt between 825 and 825+1000
		or ss_wholesale_cost between 43 and 43+20)) B2,
	(select avg(ss_list_price) B3_LP
		,count(ss_list_price) B3_CNT
		,count(distinct ss_list_price) B3_CNTD
		from store_sales
		where ss_quantity between 11 and 15
		and (ss_list_price between 74 and 74+10
		or ss_coupon_amt between 4381 and 4381+1000
		or ss_wholesale_cost between 57 and 57+20)) B3,
	(select avg(ss_list_price) B4_LP
		,count(ss_list_price) B4_CNT
		,count(distinct ss_list_price) B4_CNTD
		from store_sales
		where ss_quantity between 16 and 20
		and (ss_list_price between 89 and 89+10
		or ss_coupon_amt between 3117 and 3117+1000
		or ss_wholesale_cost between 68 and 68+20)) B4,
	(select avg(ss_list_price) B5_LP
		,count(ss_list_price) B5_CNT
		,count(distinct ss_list_price) B5_CNTD
		from store_sales
		where ss_quantity between 21 and 25
		and (ss_list_price between 58 and 58+10
		or ss_coupon_amt between 9402 and 9402+1000
		or ss_wholesale_cost between 38 and 38+20)) B5,
	(select avg(ss_list_price) B6_LP
		,count(ss_list_price) B6_CNT
		,count(distinct ss_list_price) B6_CNTD
		from store_sales
		where ss_quantity between 26 and 30
		and (ss_list_price between 64 and 64+10
		or ss_coupon_amt between 5792 and 5792+1000
		or ss_wholesale_cost between 73 and 73+20)) B6
		limit 100;`,
	},
	{
		Query: `
-- start query 29 in stream 0 using template query29.tpl
select   
     i_item_id
    ,i_item_desc
    ,s_store_id
    ,s_store_name
    ,max(ss_quantity)        as store_sales_quantity
    ,max(sr_return_quantity) as store_returns_quantity
    ,max(cs_quantity)        as catalog_sales_quantity
 from
    store_sales
   ,store_returns
   ,catalog_sales
   ,date_dim             d1
   ,date_dim             d2
   ,date_dim             d3
   ,store
   ,item
 where
     d1.d_moy               = 4 
 and d1.d_year              = 1998
 and d1.d_date_sk           = ss_sold_date_sk
 and i_item_sk              = ss_item_sk
 and s_store_sk             = ss_store_sk
 and ss_customer_sk         = sr_customer_sk
 and ss_item_sk             = sr_item_sk
 and ss_ticket_number       = sr_ticket_number
 and sr_returned_date_sk    = d2.d_date_sk
 and d2.d_moy               between 4 and  4 + 3 
 and d2.d_year              = 1998
 and sr_customer_sk         = cs_bill_customer_sk
 and sr_item_sk             = cs_item_sk
 and cs_sold_date_sk        = d3.d_date_sk     
 and d3.d_year              in (1998,1998+1,1998+2)
 group by
    i_item_id
   ,i_item_desc
   ,s_store_id
   ,s_store_name
 order by
    i_item_id 
   ,i_item_desc
   ,s_store_id
   ,s_store_name
 limit 100;`,
	},
	{
		Query: `
		-- start query 30 in stream 0 using template query30.tpl
		with customer_total_return as
	(select wr_returning_customer_sk as ctr_customer_sk
		,ca_state as ctr_state,
		sum(wr_return_amt) as ctr_total_return
		from web_returns
		,date_dim
		,customer_address
		where wr_returned_date_sk = d_date_sk
		and d_year =2000
		and wr_returning_addr_sk = ca_address_sk
		group by wr_returning_customer_sk
		,ca_state)
		select  c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
		,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
		,c_last_review_date_sk,ctr_total_return
		from customer_total_return ctr1
		,customer_address
		,customer
		where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
		from customer_total_return ctr2
		where ctr1.ctr_state = ctr2.ctr_state)
		and ca_address_sk = c_current_addr_sk
		and ca_state = 'AR'
		and ctr1.ctr_customer_sk = c_customer_sk
		order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
		,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
		,c_last_review_date_sk,ctr_total_return
		limit 100;`,
	},
	{
		Query: `
-- start query 31 in stream 0 using template query31.tpl
with ss as
 (select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as store_sales
 from store_sales,date_dim,customer_address
 where ss_sold_date_sk = d_date_sk
  and ss_addr_sk=ca_address_sk
 group by ca_county,d_qoy, d_year),
 ws as
 (select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as web_sales
 from web_sales,date_dim,customer_address
 where ws_sold_date_sk = d_date_sk
  and ws_bill_addr_sk=ca_address_sk
 group by ca_county,d_qoy, d_year)
 select 
        ss1.ca_county
       ,ss1.d_year
       ,ws2.web_sales/ws1.web_sales web_q1_q2_increase
       ,ss2.store_sales/ss1.store_sales store_q1_q2_increase
       ,ws3.web_sales/ws2.web_sales web_q2_q3_increase
       ,ss3.store_sales/ss2.store_sales store_q2_q3_increase
 from
        ss ss1
       ,ss ss2
       ,ss ss3
       ,ws ws1
       ,ws ws2
       ,ws ws3
 where
    ss1.d_qoy = 1
    and ss1.d_year = 1999
    and ss1.ca_county = ss2.ca_county
    and ss2.d_qoy = 2
    and ss2.d_year = 1999
 and ss2.ca_county = ss3.ca_county
    and ss3.d_qoy = 3
    and ss3.d_year = 1999
    and ss1.ca_county = ws1.ca_county
    and ws1.d_qoy = 1
    and ws1.d_year = 1999
    and ws1.ca_county = ws2.ca_county
    and ws2.d_qoy = 2
    and ws2.d_year = 1999
    and ws1.ca_county = ws3.ca_county
    and ws3.d_qoy = 3
    and ws3.d_year =1999
    and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end 
       > case when ss1.store_sales > 0 then ss2.store_sales/ss1.store_sales else null end
    and case when ws2.web_sales > 0 then ws3.web_sales/ws2.web_sales else null end
       > case when ss2.store_sales > 0 then ss3.store_sales/ss2.store_sales else null end
 order by store_q2_q3_increase;`,
	},
	{
		Query: `
		-- start query 32 in stream 0 using template query32.tpl
		select  sum(cs_ext_discount_amt)  as "excess discount amount"
		from
		catalog_sales
		,item
		,date_dim
		where
		i_manufact_id = 722
		and i_item_sk = cs_item_sk
		and d_date between '2001-03-09' and
	(cast('2001-03-09' as date) + interval 90 day)
		and d_date_sk = cs_sold_date_sk
		and cs_ext_discount_amt
		> (
		select
		1.3 * avg(cs_ext_discount_amt)
		from
		catalog_sales
		,date_dim
		where
		cs_item_sk = i_item_sk
		and d_date between '2001-03-09' and
	(cast('2001-03-09' as date) + interval 90 day)
		and d_date_sk = cs_sold_date_sk
	)
		limit 100;`,
	},
	{
		Query: `
-- start query 33 in stream 0 using template query33.tpl
with ss as (
 select
          i_manufact_id,sum(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 where
         i_manufact_id in (select
  i_manufact_id
from
 item
where i_category in ('Books'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 3
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -5 
 group by i_manufact_id),
 cs as (
 select
          i_manufact_id,sum(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 where
         i_manufact_id               in (select
  i_manufact_id
from
 item
where i_category in ('Books'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 3
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -5 
 group by i_manufact_id),
 ws as (
 select
          i_manufact_id,sum(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 where
         i_manufact_id               in (select
  i_manufact_id
from
 item
where i_category in ('Books'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 3
 and     ws_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -5
 group by i_manufact_id)
  select  i_manufact_id ,sum(total_sales) total_sales
 from  (select * from ss 
        union all
        select * from cs 
        union all
        select * from ws) tmp1
 group by i_manufact_id
 order by total_sales
limit 100;`,
	},
	{
		Query: `
		-- start query 34 in stream 0 using template query34.tpl
		select c_last_name
		,c_first_name
		,c_salutation
		,c_preferred_cust_flag
		,ss_ticket_number
		,cnt from
	(select ss_ticket_number
		,ss_customer_sk
		,count(*) cnt
		from store_sales,date_dim,store,household_demographics
		where store_sales.ss_sold_date_sk = date_dim.d_date_sk
		and store_sales.ss_store_sk = store.s_store_sk
		and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
		and (date_dim.d_dom between 1 and 3 or date_dim.d_dom between 25 and 28)
		and (household_demographics.hd_buy_potential = '1001-5000' or
		household_demographics.hd_buy_potential = '0-500')
		and household_demographics.hd_vehicle_count > 0
		and (case when household_demographics.hd_vehicle_count > 0
		then household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count
		else null
		end)  > 1.2
		and date_dim.d_year in (2000,2000+1,2000+2)
		and store.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County',
		'Williamson County','Williamson County','Williamson County','Williamson County')
		group by ss_ticket_number,ss_customer_sk) dn,customer
		where ss_customer_sk = c_customer_sk
		and cnt between 15 and 20
		order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc, ss_ticket_number;`,
	},
	{
		Query: `
-- start query 35 in stream 0 using template query35.tpl
select   
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  count(*) cnt1,
  avg(cd_dep_count),
  stddev_samp(cd_dep_count),
  sum(cd_dep_count),
  cd_dep_employed_count,
  count(*) cnt2,
  avg(cd_dep_employed_count),
  stddev_samp(cd_dep_employed_count),
  sum(cd_dep_employed_count),
  cd_dep_college_count,
  count(*) cnt3,
  avg(cd_dep_college_count),
  stddev_samp(cd_dep_college_count),
  sum(cd_dep_college_count)
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 1999 and
                d_qoy < 4) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 1999 and
                  d_qoy < 4) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 1999 and
                  d_qoy < 4))
 group by ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 limit 100;`,
	},
	{
		Query: `
		-- start query 36 in stream 0 using template query36.tpl
		select
		sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
		,i_category
		,i_class
		,grouping(i_category)+grouping(i_class) as lochierarchy
		,rank() over (
		partition by grouping(i_category)+grouping(i_class),
	case when grouping(i_class) = 0 then i_category end
		order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
		from
		store_sales
		,date_dim       d1
		,item
		,store
		where
		d1.d_year = 2000
		and d1.d_date_sk = ss_sold_date_sk
		and i_item_sk  = ss_item_sk
		and s_store_sk  = ss_store_sk
		and s_state in ('TN','TN','TN','TN',
		'TN','TN','TN','TN')
		group by rollup(i_category,i_class)
		order by
		lochierarchy desc
		,case when lochierarchy = 0 then i_category end
		,rank_within_parent
		limit 100;`,
		Skip: true,
	},
	{
		Query: `
-- start query 37 in stream 0 using template query37.tpl
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, catalog_sales
 where i_current_price between 29 and 29 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2002-03-29' as date) and (cast('2002-03-29' as date) +  interval 60 day)
 and i_manufact_id in (705,742,777,944)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;`,
	},
	{
		Query: `
		-- start query 38 in stream 0 using template query38.tpl
		select  count(*) from (
		select distinct c_last_name, c_first_name, d_date
		from store_sales, date_dim, customer
		where store_sales.ss_sold_date_sk = date_dim.d_date_sk
		and store_sales.ss_customer_sk = customer.c_customer_sk
		and d_month_seq between 1189 and 1189 + 11
		intersect
		select distinct c_last_name, c_first_name, d_date
		from catalog_sales, date_dim, customer
		where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
		and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
		and d_month_seq between 1189 and 1189 + 11
		intersect
		select distinct c_last_name, c_first_name, d_date
		from web_sales, date_dim, customer
		where web_sales.ws_sold_date_sk = date_dim.d_date_sk
		and web_sales.ws_bill_customer_sk = customer.c_customer_sk
		and d_month_seq between 1189 and 1189 + 11
	) hot_cust
		limit 100;`,
	},
	{
		Query: `
-- start query 39 in stream 0 using template query39.tpl
with inv as
(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0 then null else stdev/mean end cov
 from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from inventory
          ,item
          ,warehouse
          ,date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year =2000
      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
 where case mean when 0 then 0 else stdev/mean end > 1)
select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from inv inv1,inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=1
  and inv2.d_moy=1+1
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
        ,inv2.d_moy,inv2.mean, inv2.cov
;
with inv as
(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0 then null else stdev/mean end cov
 from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from inventory
          ,item
          ,warehouse
          ,date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year =2000
      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
 where case mean when 0 then 0 else stdev/mean end > 1)
select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from inv inv1,inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=1
  and inv2.d_moy=1+1
  and inv1.cov > 1.5
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
        ,inv2.d_moy,inv2.mean, inv2.cov
;`,
	},
	{
		Query: `
		-- start query 40 in stream 0 using template query40.tpl
		select
		w_state
		,i_item_id
		,sum(case when (cast(d_date as date) < cast ('2001-05-02' as date))
		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
		,sum(case when (cast(d_date as date) >= cast ('2001-05-02' as date))
		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
		from
		catalog_sales left outer join catalog_returns on
	(cs_order_number = cr_order_number
		and cs_item_sk = cr_item_sk)
		,warehouse
		,item
		,date_dim
		where
		i_current_price between 0.99 and 1.49
		and i_item_sk          = cs_item_sk
		and cs_warehouse_sk    = w_warehouse_sk
		and cs_sold_date_sk    = d_date_sk
		and d_date between (cast ('2001-05-02' as date) - interval 30 day)
		and (cast ('2001-05-02' as date) + interval 30 day)
		group by
		w_state,i_item_id
		order by w_state,i_item_id
		limit 100;`,
	},
	{
		Query: `
-- start query 41 in stream 0 using template query41.tpl
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 704 and 704+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'forest' or i_color = 'lime') and 
        (i_units = 'Pallet' or i_units = 'Pound') and
        (i_size = 'economy' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'navy' or i_color = 'slate') and
        (i_units = 'Gross' or i_units = 'Bunch') and
        (i_size = 'extra large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'powder' or i_color = 'sky') and
        (i_units = 'Dozen' or i_units = 'Lb') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'maroon' or i_color = 'smoke') and
        (i_units = 'Ounce' or i_units = 'Case') and
        (i_size = 'economy' or i_size = 'small')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'dark' or i_color = 'aquamarine') and 
        (i_units = 'Ton' or i_units = 'Tbl') and
        (i_size = 'economy' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'frosted' or i_color = 'plum') and
        (i_units = 'Dram' or i_units = 'Box') and
        (i_size = 'extra large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'papaya' or i_color = 'peach') and
        (i_units = 'Bundle' or i_units = 'Carton') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'firebrick' or i_color = 'sienna') and
        (i_units = 'Cup' or i_units = 'Each') and
        (i_size = 'economy' or i_size = 'small')
        )))) > 0
 order by i_product_name
 limit 100;`,
	},
	{
		Query: `
		-- start query 42 in stream 0 using template query42.tpl
		select  dt.d_year
		,item.i_category_id
		,item.i_category
		,sum(ss_ext_sales_price)
		from 	date_dim dt
		,store_sales
		,item
		where dt.d_date_sk = store_sales.ss_sold_date_sk
		and store_sales.ss_item_sk = item.i_item_sk
		and item.i_manager_id = 1
		and dt.d_moy=11
		and dt.d_year=1998
		group by 	dt.d_year
		,item.i_category_id
		,item.i_category
		order by       sum(ss_ext_sales_price) desc,dt.d_year
		,item.i_category_id
		,item.i_category
		limit 100 ;`,
	},
	{
		Query: `
-- start query 43 in stream 0 using template query43.tpl
select  s_store_name, s_store_id,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
 from date_dim, store_sales, store
 where d_date_sk = ss_sold_date_sk and
       s_store_sk = ss_store_sk and
       s_gmt_offset = -5 and
       d_year = 2000 
 group by s_store_name, s_store_id
 order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales
 limit 100;`,
	},
	{
		Query: `
		-- start query 44 in stream 0 using template query44.tpl
		select  asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
		from(select *
		from (select item_sk,rank() over (order by rank_col asc) rnk
		from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
		from store_sales ss1
		where ss_store_sk = 4
		group by ss_item_sk
		having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
		from store_sales
		where ss_store_sk = 4
		and ss_hdemo_sk is null
		group by ss_store_sk))V1)V11
		where rnk  < 11) asceding,
	(select *
		from (select item_sk,rank() over (order by rank_col desc) rnk
		from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
		from store_sales ss1
		where ss_store_sk = 4
		group by ss_item_sk
		having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
		from store_sales
		where ss_store_sk = 4
		and ss_hdemo_sk is null
		group by ss_store_sk))V2)V21
		where rnk  < 11) descending,
		item i1,
		item i2
		where asceding.rnk = descending.rnk
		and i1.i_item_sk=asceding.item_sk
		and i2.i_item_sk=descending.item_sk
		order by asceding.rnk
		limit 100;`,
	},
	{
		Query: `
-- start query 45 in stream 0 using template query45.tpl
select  ca_zip, ca_city, sum(ws_sales_price)
 from web_sales, customer, customer_address, date_dim, item
 where ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ws_item_sk = i_item_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	      or 
 	      i_item_id in (select i_item_id
                             from item
                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                             )
 	    )
 	and ws_sold_date_sk = d_date_sk
 	and d_qoy = 1 and d_year = 2000
 group by ca_zip, ca_city
 order by ca_zip, ca_city
 limit 100;`,
	},
	{
		Query: `
		-- start query 46 in stream 0 using template query46.tpl
		select  c_last_name
		,c_first_name
		,ca_city
		,bought_city
		,ss_ticket_number
		,amt,profit
		from
	(select ss_ticket_number
		,ss_customer_sk
		,ca_city bought_city
		,sum(ss_coupon_amt) amt
		,sum(ss_net_profit) profit
		from store_sales,date_dim,store,household_demographics,customer_address
		where store_sales.ss_sold_date_sk = date_dim.d_date_sk
		and store_sales.ss_store_sk = store.s_store_sk
		and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
		and store_sales.ss_addr_sk = customer_address.ca_address_sk
		and (household_demographics.hd_dep_count = 8 or
		household_demographics.hd_vehicle_count= 0)
		and date_dim.d_dow in (6,0)
		and date_dim.d_year in (2000,2000+1,2000+2)
		and store.s_city in ('Midway','Fairview','Fairview','Fairview','Fairview')
		group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,customer,customer_address current_addr
		where ss_customer_sk = c_customer_sk
		and customer.c_current_addr_sk = current_addr.ca_address_sk
		and current_addr.ca_city <> bought_city
		order by c_last_name
		,c_first_name
		,ca_city
		,bought_city
		,ss_ticket_number
		limit 100;`,
	},
	{
		Query: `
-- start query 47 in stream 0 using template query47.tpl
with v1 as(
 select i_category, i_brand,
        s_store_name, s_company_name,
        d_year, d_moy,
        sum(ss_sales_price) sum_sales,
        avg(sum(ss_sales_price)) over
          (partition by i_category, i_brand,
                     s_store_name, s_company_name, d_year)
          avg_monthly_sales,
        rank() over
          (partition by i_category, i_brand,
                     s_store_name, s_company_name
           order by d_year, d_moy) rn
 from item, store_sales, date_dim, store
 where ss_item_sk = i_item_sk and
       ss_sold_date_sk = d_date_sk and
       ss_store_sk = s_store_sk and
       (
         d_year = 2000 or
         ( d_year = 2000-1 and d_moy =12) or
         ( d_year = 2000+1 and d_moy =1)
       )
 group by i_category, i_brand,
          s_store_name, s_company_name,
          d_year, d_moy),
 v2 as(
 select v1.s_store_name, v1.s_company_name
        ,v1.d_year
        ,v1.avg_monthly_sales
        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from v1, v1 v1_lag, v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1.s_store_name = v1_lag.s_store_name and
       v1.s_store_name = v1_lead.s_store_name and
       v1.s_company_name = v1_lag.s_company_name and
       v1.s_company_name = v1_lead.s_company_name and
       v1.rn = v1_lag.rn + 1 and
       v1.rn = v1_lead.rn - 1)
  select  *
 from v2
 where  d_year = 2000 and    
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
 order by sum_sales - avg_monthly_sales, nsum
 limit 100;`,
	},
	{
		Query: `
		-- start query 48 in stream 0 using template query48.tpl
		select sum (ss_quantity)
		from store_sales, store, customer_demographics, customer_address, date_dim
		where s_store_sk = ss_store_sk
		and  ss_sold_date_sk = d_date_sk and d_year = 2001
		and
	(
	(
		cd_demo_sk = ss_cdemo_sk
		and
		cd_marital_status = 'S'
		and
		cd_education_status = 'Secondary'
		and
		ss_sales_price between 100.00 and 150.00
	)
		or
	(
		cd_demo_sk = ss_cdemo_sk
		and
		cd_marital_status = 'M'
		and
		cd_education_status = '2 yr Degree'
		and
		ss_sales_price between 50.00 and 100.00
	)
		or
	(
		cd_demo_sk = ss_cdemo_sk
		and
		cd_marital_status = 'D'
		and
		cd_education_status = 'Advanced Degree'
		and
		ss_sales_price between 150.00 and 200.00
	)
	)
		and
	(
	(
		ss_addr_sk = ca_address_sk
		and
		ca_country = 'United States'
		and
		ca_state in ('ND', 'NY', 'SD')
		and ss_net_profit between 0 and 2000
	)
		or
	(ss_addr_sk = ca_address_sk
		and
		ca_country = 'United States'
		and
		ca_state in ('MD', 'GA', 'KS')
		and ss_net_profit between 150 and 3000
	)
		or
	(ss_addr_sk = ca_address_sk
		and
		ca_country = 'United States'
		and
		ca_state in ('CO', 'MN', 'NC')
		and ss_net_profit between 50 and 25000
	)
	)
		;`,
	},
	{
		Query: `
-- start query 49 in stream 0 using template query49.tpl
select  channel, item, return_ratio, return_rank, currency_rank from
 (select
 'web' as channel
 ,web.item
 ,web.return_ratio
 ,web.return_rank
 ,web.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select ws.ws_item_sk as item
 		,(cast(sum(coalesce(wr.wr_return_quantity,0)) as decimal(15,4))/
 		cast(sum(coalesce(ws.ws_quantity,0)) as decimal(15,4) )) as return_ratio
 		,(cast(sum(coalesce(wr.wr_return_amt,0)) as decimal(15,4))/
 		cast(sum(coalesce(ws.ws_net_paid,0)) as decimal(15,4) )) as currency_ratio
 		from 
 		 web_sales ws left outer join web_returns wr 
 			on (ws.ws_order_number = wr.wr_order_number and 
 			ws.ws_item_sk = wr.wr_item_sk)
                 ,date_dim
 		where 
 			wr.wr_return_amt > 10000 
 			and ws.ws_net_profit > 1
                         and ws.ws_net_paid > 0
                         and ws.ws_quantity > 0
                         and ws_sold_date_sk = d_date_sk
                         and d_year = 1998
                         and d_moy = 11
 		group by ws.ws_item_sk
 	) in_web
 ) web
 where 
 (
 web.return_rank <= 10
 or
 web.currency_rank <= 10
 )
 union
 select 
 'catalog' as channel
 ,catalog.item
 ,catalog.return_ratio
 ,catalog.return_rank
 ,catalog.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select 
 		cs.cs_item_sk as item
 		,(cast(sum(coalesce(cr.cr_return_quantity,0)) as decimal(15,4))/
 		cast(sum(coalesce(cs.cs_quantity,0)) as decimal(15,4) )) as return_ratio
 		,(cast(sum(coalesce(cr.cr_return_amount,0)) as decimal(15,4))/
 		cast(sum(coalesce(cs.cs_net_paid,0)) as decimal(15,4) )) as currency_ratio
 		from 
 		catalog_sales cs left outer join catalog_returns cr
 			on (cs.cs_order_number = cr.cr_order_number and 
 			cs.cs_item_sk = cr.cr_item_sk)
                ,date_dim
 		where 
 			cr.cr_return_amount > 10000 
 			and cs.cs_net_profit > 1
                         and cs.cs_net_paid > 0
                         and cs.cs_quantity > 0
                         and cs_sold_date_sk = d_date_sk
                         and d_year = 1998
                         and d_moy = 11
                 group by cs.cs_item_sk
 	) in_cat
 ) catalog
 where 
 (
 catalog.return_rank <= 10
 or
 catalog.currency_rank <=10
 )
 union
 select 
 'store' as channel
 ,store.item
 ,store.return_ratio
 ,store.return_rank
 ,store.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select sts.ss_item_sk as item
 		,(cast(sum(coalesce(sr.sr_return_quantity,0)) as decimal(15,4))/cast(sum(coalesce(sts.ss_quantity,0)) as decimal(15,4) )) as return_ratio
 		,(cast(sum(coalesce(sr.sr_return_amt,0)) as decimal(15,4))/cast(sum(coalesce(sts.ss_net_paid,0)) as decimal(15,4) )) as currency_ratio
 		from 
 		store_sales sts left outer join store_returns sr
 			on (sts.ss_ticket_number = sr.sr_ticket_number and sts.ss_item_sk = sr.sr_item_sk)
                ,date_dim
 		where 
 			sr.sr_return_amt > 10000 
 			and sts.ss_net_profit > 1
                         and sts.ss_net_paid > 0 
                         and sts.ss_quantity > 0
                         and ss_sold_date_sk = d_date_sk
                         and d_year = 1998
                         and d_moy = 11
 		group by sts.ss_item_sk
 	) in_store
 ) store
 where  (
 store.return_rank <= 10
 or 
 store.currency_rank <= 10
 )
 )
 order by 1,4,5,2
 limit 100;`,
	},
	{
		Query: `
		-- start query 50 in stream 0 using template query50.tpl
		select
		s_store_name
		,s_company_id
		,s_street_number
		,s_street_name
		,s_street_type
		,s_suite_number
		,s_city
		,s_county
		,s_state
		,s_zip
		,sum(case when (sr_returned_date_sk - ss_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days"
		,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 30) and
	(sr_returned_date_sk - ss_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days"
		,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 60) and
	(sr_returned_date_sk - ss_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days"
		,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 90) and
	(sr_returned_date_sk - ss_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days"
		,sum(case when (sr_returned_date_sk - ss_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days"
		from
		store_sales
		,store_returns
		,store
		,date_dim d1
		,date_dim d2
		where
		d2.d_year = 2001
		and d2.d_moy  = 8
		and ss_ticket_number = sr_ticket_number
		and ss_item_sk = sr_item_sk
		and ss_sold_date_sk   = d1.d_date_sk
		and sr_returned_date_sk   = d2.d_date_sk
		and ss_customer_sk = sr_customer_sk
		and ss_store_sk = s_store_sk
		group by
		s_store_name
		,s_company_id
		,s_street_number
		,s_street_name
		,s_street_type
		,s_suite_number
		,s_city
		,s_county
		,s_state
		,s_zip
		order by s_store_name
		,s_company_id
		,s_street_number
		,s_street_name
		,s_street_type
		,s_suite_number
		,s_city
		,s_county
		,s_state
		,s_zip
		limit 100;`,
	},
	{
		Query: `
-- start query 51 in stream 0 using template query51.tpl
WITH web_v1 as (
select
  ws_item_sk item_sk, d_date,
  sum(sum(ws_sales_price))
      over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
from web_sales
    ,date_dim
where ws_sold_date_sk=d_date_sk
  and d_month_seq between 1212 and 1212+11
  and ws_item_sk is not NULL
group by ws_item_sk, d_date),
store_v1 as (
select
  ss_item_sk item_sk, d_date,
  sum(sum(ss_sales_price))
      over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
from store_sales
    ,date_dim
where ss_sold_date_sk=d_date_sk
  and d_month_seq between 1212 and 1212+11
  and ss_item_sk is not NULL
group by ss_item_sk, d_date)
 select  *
from (select item_sk
     ,d_date
     ,web_sales
     ,store_sales
     ,max(web_sales)
         over (partition by item_sk order by d_date rows between unbounded preceding and current row) web_cumulative
     ,max(store_sales)
         over (partition by item_sk order by d_date rows between unbounded preceding and current row) store_cumulative
     from (select case when web.item_sk is not null then web.item_sk else store.item_sk end item_sk
                 ,case when web.d_date is not null then web.d_date else store.d_date end d_date
                 ,web.cume_sales web_sales
                 ,store.cume_sales store_sales
           from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_sk
                                                          and web.d_date = store.d_date)
          )x )y
where web_cumulative > store_cumulative
order by item_sk
        ,d_date
limit 100;`,
	},
	{
		Query: `
		-- start query 52 in stream 0 using template query52.tpl
		select  dt.d_year
		,item.i_brand_id brand_id
		,item.i_brand brand
		,sum(ss_ext_sales_price) ext_price
		from date_dim dt
		,store_sales
		,item
		where dt.d_date_sk = store_sales.ss_sold_date_sk
		and store_sales.ss_item_sk = item.i_item_sk
		and item.i_manager_id = 1
		and dt.d_moy=12
		and dt.d_year=2000
		group by dt.d_year
		,item.i_brand
		,item.i_brand_id
		order by dt.d_year
		,ext_price desc
		,brand_id
		limit 100 ;`,
	},
	{
		Query: `
-- start query 53 in stream 0 using template query53.tpl
select  * from 
(select i_manufact_id,
sum(ss_sales_price) sum_sales,
avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
ss_sold_date_sk = d_date_sk and
ss_store_sk = s_store_sk and
d_month_seq in (1186,1186+1,1186+2,1186+3,1186+4,1186+5,1186+6,1186+7,1186+8,1186+9,1186+10,1186+11) and
((i_category in ('Books','Children','Electronics') and
i_class in ('personal','portable','reference','self-help') and
i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		'exportiunivamalg #9','scholaramalgamalg #9'))
or(i_category in ('Women','Music','Men') and
i_class in ('accessories','classical','fragrances','pants') and
i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		'importoamalg #1')))
group by i_manufact_id, d_qoy ) tmp1
where case when avg_quarterly_sales > 0 
	then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales 
	else null end > 0.1
order by avg_quarterly_sales,
	 sum_sales,
	 i_manufact_id
limit 100;`,
	},
	{
		Query: `
		-- start query 54 in stream 0 using template query54.tpl
		with my_customers as (
		select distinct c_customer_sk
		, c_current_addr_sk
		from
	( select cs_sold_date_sk sold_date_sk,
		cs_bill_customer_sk customer_sk,
		cs_item_sk item_sk
		from   catalog_sales
		union all
		select ws_sold_date_sk sold_date_sk,
		ws_bill_customer_sk customer_sk,
		ws_item_sk item_sk
		from   web_sales
	) cs_or_ws_sales,
		item,
		date_dim,
		customer
		where   sold_date_sk = d_date_sk
		and item_sk = i_item_sk
		and i_category = 'Music'
		and i_class = 'country'
		and c_customer_sk = cs_or_ws_sales.customer_sk
		and d_moy = 1
		and d_year = 1999
	)
		, my_revenue as (
		select c_customer_sk,
		sum(ss_ext_sales_price) as revenue
		from   my_customers,
		store_sales,
		customer_address,
		store,
		date_dim
		where  c_current_addr_sk = ca_address_sk
		and ca_county = s_county
		and ca_state = s_state
		and ss_sold_date_sk = d_date_sk
		and c_customer_sk = ss_customer_sk
		and d_month_seq between (select distinct d_month_seq+1
		from   date_dim where d_year = 1999 and d_moy = 1)
		and  (select distinct d_month_seq+3
		from   date_dim where d_year = 1999 and d_moy = 1)
		group by c_customer_sk
	)
		, segments as
	(select cast((revenue/50) as int) as segment
		from   my_revenue
	)
		select  segment, count(*) as num_customers, segment*50 as segment_base
		from segments
		group by segment
		order by segment, num_customers
		limit 100;`,
	},
	{
		Query: `
-- start query 55 in stream 0 using template query55.tpl
select  i_brand_id brand_id, i_brand brand,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item
 where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id=52
 	and d_moy=11
 	and d_year=2000
 group by i_brand, i_brand_id
 order by ext_price desc, i_brand_id
limit 100 ;`,
	},
	{
		Query: `
		-- start query 56 in stream 0 using template query56.tpl
		with ss as (
		select i_item_id,sum(ss_ext_sales_price) total_sales
		from
		store_sales,
		date_dim,
		customer_address,
		item
		where i_item_id in (select
		i_item_id
		from item
		where i_color in ('powder','orchid','pink'))
		and     ss_item_sk              = i_item_sk
		and     ss_sold_date_sk         = d_date_sk
		and     d_year                  = 2000
		and     d_moy                   = 3
		and     ss_addr_sk              = ca_address_sk
		and     ca_gmt_offset           = -6
		group by i_item_id),
		cs as (
		select i_item_id,sum(cs_ext_sales_price) total_sales
		from
		catalog_sales,
		date_dim,
		customer_address,
		item
		where
		i_item_id               in (select
		i_item_id
		from item
		where i_color in ('powder','orchid','pink'))
		and     cs_item_sk              = i_item_sk
		and     cs_sold_date_sk         = d_date_sk
		and     d_year                  = 2000
		and     d_moy                   = 3
		and     cs_bill_addr_sk         = ca_address_sk
		and     ca_gmt_offset           = -6
		group by i_item_id),
		ws as (
		select i_item_id,sum(ws_ext_sales_price) total_sales
		from
		web_sales,
		date_dim,
		customer_address,
		item
		where
		i_item_id               in (select
		i_item_id
		from item
		where i_color in ('powder','orchid','pink'))
		and     ws_item_sk              = i_item_sk
		and     ws_sold_date_sk         = d_date_sk
		and     d_year                  = 2000
		and     d_moy                   = 3
		and     ws_bill_addr_sk         = ca_address_sk
		and     ca_gmt_offset           = -6
		group by i_item_id)
		select  i_item_id ,sum(total_sales) total_sales
		from  (select * from ss
		union all
		select * from cs
		union all
		select * from ws) tmp1
		group by i_item_id
		order by total_sales,
		i_item_id
		limit 100;`,
	},
	{
		Query: `
-- start query 57 in stream 0 using template query57.tpl
with v1 as(
 select i_category, i_brand,
        cc_name,
        d_year, d_moy,
        sum(cs_sales_price) sum_sales,
        avg(sum(cs_sales_price)) over
          (partition by i_category, i_brand,
                     cc_name, d_year)
          avg_monthly_sales,
        rank() over
          (partition by i_category, i_brand,
                     cc_name
           order by d_year, d_moy) rn
 from item, catalog_sales, date_dim, call_center
 where cs_item_sk = i_item_sk and
       cs_sold_date_sk = d_date_sk and
       cc_call_center_sk= cs_call_center_sk and
       (
         d_year = 2001 or
         ( d_year = 2001-1 and d_moy =12) or
         ( d_year = 2001+1 and d_moy =1)
       )
 group by i_category, i_brand,
          cc_name , d_year, d_moy),
 v2 as(
 select v1.i_category, v1.i_brand, v1.cc_name
        ,v1.d_year
        ,v1.avg_monthly_sales
        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from v1, v1 v1_lag, v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1. cc_name = v1_lag. cc_name and
       v1. cc_name = v1_lead. cc_name and
       v1.rn = v1_lag.rn + 1 and
       v1.rn = v1_lead.rn - 1)
  select  *
 from v2
 where  d_year = 2001 and
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
 order by sum_sales - avg_monthly_sales, avg_monthly_sales
 limit 100;`,
	},
	{
		Query: `
		-- start query 58 in stream 0 using template query58.tpl
		with ss_items as
	(select i_item_id item_id
		,sum(ss_ext_sales_price) ss_item_rev
		from store_sales
		,item
		,date_dim
		where ss_item_sk = i_item_sk
		and d_date in (select d_date
		from date_dim
		where d_week_seq = (select d_week_seq
		from date_dim
		where d_date = '2001-06-16'))
		and ss_sold_date_sk   = d_date_sk
		group by i_item_id),
		cs_items as
	(select i_item_id item_id
		,sum(cs_ext_sales_price) cs_item_rev
		from catalog_sales
		,item
		,date_dim
		where cs_item_sk = i_item_sk
		and  d_date in (select d_date
		from date_dim
		where d_week_seq = (select d_week_seq
		from date_dim
		where d_date = '2001-06-16'))
		and  cs_sold_date_sk = d_date_sk
		group by i_item_id),
		ws_items as
	(select i_item_id item_id
		,sum(ws_ext_sales_price) ws_item_rev
		from web_sales
		,item
		,date_dim
		where ws_item_sk = i_item_sk
		and  d_date in (select d_date
		from date_dim
		where d_week_seq =(select d_week_seq
		from date_dim
		where d_date = '2001-06-16'))
		and ws_sold_date_sk   = d_date_sk
		group by i_item_id)
		select  ss_items.item_id
		,ss_item_rev
		,ss_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ss_dev
		,cs_item_rev
		,cs_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 cs_dev
		,ws_item_rev
		,ws_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ws_dev
		,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
		from ss_items,cs_items,ws_items
		where ss_items.item_id=cs_items.item_id
		and ss_items.item_id=ws_items.item_id
		and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
		and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
		and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
		and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
		and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
		and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
		order by item_id
		,ss_item_rev
		limit 100;`,
	},
	{
		Query: `
-- start query 59 in stream 0 using template query59.tpl
with wss as 
 (select d_week_seq,
        ss_store_sk,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
 from store_sales,date_dim
 where d_date_sk = ss_sold_date_sk
 group by d_week_seq,ss_store_sk
 )
  select  s_store_name1,s_store_id1,d_week_seq1
       ,sun_sales1/sun_sales2,mon_sales1/mon_sales2
       ,tue_sales1/tue_sales2,wed_sales1/wed_sales2,thu_sales1/thu_sales2
       ,fri_sales1/fri_sales2,sat_sales1/sat_sales2
 from
 (select s_store_name s_store_name1,wss.d_week_seq d_week_seq1
        ,s_store_id s_store_id1,sun_sales sun_sales1
        ,mon_sales mon_sales1,tue_sales tue_sales1
        ,wed_sales wed_sales1,thu_sales thu_sales1
        ,fri_sales fri_sales1,sat_sales sat_sales1
  from wss,store,date_dim d
  where d.d_week_seq = wss.d_week_seq and
        ss_store_sk = s_store_sk and 
        d_month_seq between 1195 and 1195 + 11) y,
 (select s_store_name s_store_name2,wss.d_week_seq d_week_seq2
        ,s_store_id s_store_id2,sun_sales sun_sales2
        ,mon_sales mon_sales2,tue_sales tue_sales2
        ,wed_sales wed_sales2,thu_sales thu_sales2
        ,fri_sales fri_sales2,sat_sales sat_sales2
  from wss,store,date_dim d
  where d.d_week_seq = wss.d_week_seq and
        ss_store_sk = s_store_sk and 
        d_month_seq between 1195+ 12 and 1195 + 23) x
 where s_store_id1=s_store_id2
   and d_week_seq1=d_week_seq2-52
 order by s_store_name1,s_store_id1,d_week_seq1
limit 100;`,
	},
	{
		Query: `
		-- start query 60 in stream 0 using template query60.tpl
		with ss as (
		select
		i_item_id,sum(ss_ext_sales_price) total_sales
		from
		store_sales,
		date_dim,
		customer_address,
		item
		where
		i_item_id in (select
		i_item_id
		from
		item
		where i_category in ('Jewelry'))
		and     ss_item_sk              = i_item_sk
		and     ss_sold_date_sk         = d_date_sk
		and     d_year                  = 2000
		and     d_moy                   = 10
		and     ss_addr_sk              = ca_address_sk
		and     ca_gmt_offset           = -5
		group by i_item_id),
		cs as (
		select
		i_item_id,sum(cs_ext_sales_price) total_sales
		from
		catalog_sales,
		date_dim,
		customer_address,
		item
		where
		i_item_id               in (select
		i_item_id
		from
		item
		where i_category in ('Jewelry'))
		and     cs_item_sk              = i_item_sk
		and     cs_sold_date_sk         = d_date_sk
		and     d_year                  = 2000
		and     d_moy                   = 10
		and     cs_bill_addr_sk         = ca_address_sk
		and     ca_gmt_offset           = -5
		group by i_item_id),
		ws as (
		select
		i_item_id,sum(ws_ext_sales_price) total_sales
		from
		web_sales,
		date_dim,
		customer_address,
		item
		where
		i_item_id               in (select
		i_item_id
		from
		item
		where i_category in ('Jewelry'))
		and     ws_item_sk              = i_item_sk
		and     ws_sold_date_sk         = d_date_sk
		and     d_year                  = 2000
		and     d_moy                   = 10
		and     ws_bill_addr_sk         = ca_address_sk
		and     ca_gmt_offset           = -5
		group by i_item_id)
		select
		i_item_id
		,sum(total_sales) total_sales
		from  (select * from ss
		union all
		select * from cs
		union all
		select * from ws) tmp1
		group by i_item_id
		order by i_item_id
		,total_sales
		limit 100;`,
	},
	{
		Query: `
-- start query 61 in stream 0 using template query61.tpl
select  promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100
from
  (select sum(ss_ext_sales_price) promotions
   from  store_sales
        ,store
        ,promotion
        ,date_dim
        ,customer
        ,customer_address 
        ,item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_promo_sk = p_promo_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk 
   and   ca_gmt_offset = -7
   and   i_category = 'Home'
   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
   and   s_gmt_offset = -7
   and   d_year = 2000
   and   d_moy  = 12) promotional_sales,
  (select sum(ss_ext_sales_price) total
   from  store_sales
        ,store
        ,date_dim
        ,customer
        ,customer_address
        ,item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -7
   and   i_category = 'Home'
   and   s_gmt_offset = -7
   and   d_year = 2000
   and   d_moy  = 12) all_sales
order by promotions, total
limit 100;`,
	},
	{
		Query: `
		-- start query 62 in stream 0 using template query62.tpl
		select
		substr(w_warehouse_name,1,20)
		,sm_type
		,web_name
		,sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days"
		,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and
	(ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days"
		,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and
	(ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days"
		,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and
	(ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days"
		,sum(case when (ws_ship_date_sk - ws_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days"
		from
		web_sales
		,warehouse
		,ship_mode
		,web_site
		,date_dim
		where
		d_month_seq between 1223 and 1223 + 11
		and ws_ship_date_sk   = d_date_sk
		and ws_warehouse_sk   = w_warehouse_sk
		and ws_ship_mode_sk   = sm_ship_mode_sk
		and ws_web_site_sk    = web_site_sk
		group by
		substr(w_warehouse_name,1,20)
		,sm_type
		,web_name
		order by substr(w_warehouse_name,1,20)
		,sm_type
		,web_name
		limit 100;`,
	},
	{
		Query: `
-- start query 63 in stream 0 using template query63.tpl
select  * 
from (select i_manager_id
             ,sum(ss_sales_price) sum_sales
             ,avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales
      from item
          ,store_sales
          ,date_dim
          ,store
      where ss_item_sk = i_item_sk
        and ss_sold_date_sk = d_date_sk
        and ss_store_sk = s_store_sk
        and d_month_seq in (1222,1222+1,1222+2,1222+3,1222+4,1222+5,1222+6,1222+7,1222+8,1222+9,1222+10,1222+11)
        and ((    i_category in ('Books','Children','Electronics')
              and i_class in ('personal','portable','reference','self-help')
              and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		                  'exportiunivamalg #9','scholaramalgamalg #9'))
           or(    i_category in ('Women','Music','Men')
              and i_class in ('accessories','classical','fragrances','pants')
              and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		                 'importoamalg #1')))
group by i_manager_id, d_moy) tmp1
where case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
order by i_manager_id
        ,avg_monthly_sales
        ,sum_sales
limit 100;`,
	},
	{
		Query: `
		-- start query 64 in stream 0 using template query64.tpl
		with cs_ui as
	(select cs_item_sk
		,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
		from catalog_sales
		,catalog_returns
		where cs_item_sk = cr_item_sk
		and cs_order_number = cr_order_number
		group by cs_item_sk
		having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
		cross_sales as
	(select i_product_name product_name
		,i_item_sk item_sk
		,s_store_name store_name
		,s_zip store_zip
		,ad1.ca_street_number b_street_number
		,ad1.ca_street_name b_street_name
		,ad1.ca_city b_city
		,ad1.ca_zip b_zip
		,ad2.ca_street_number c_street_number
		,ad2.ca_street_name c_street_name
		,ad2.ca_city c_city
		,ad2.ca_zip c_zip
		,d1.d_year as syear
		,d2.d_year as fsyear
		,d3.d_year s2year
		,count(*) cnt
		,sum(ss_wholesale_cost) s1
		,sum(ss_list_price) s2
		,sum(ss_coupon_amt) s3
		FROM   store_sales
		,store_returns
		,cs_ui
		,date_dim d1
		,date_dim d2
		,date_dim d3
		,store
		,customer
		,customer_demographics cd1
		,customer_demographics cd2
		,promotion
		,household_demographics hd1
		,household_demographics hd2
		,customer_address ad1
		,customer_address ad2
		,income_band ib1
		,income_band ib2
		,item
		WHERE  ss_store_sk = s_store_sk AND
		ss_sold_date_sk = d1.d_date_sk AND
		ss_customer_sk = c_customer_sk AND
		ss_cdemo_sk= cd1.cd_demo_sk AND
		ss_hdemo_sk = hd1.hd_demo_sk AND
		ss_addr_sk = ad1.ca_address_sk and
		ss_item_sk = i_item_sk and
		ss_item_sk = sr_item_sk and
		ss_ticket_number = sr_ticket_number and
		ss_item_sk = cs_ui.cs_item_sk and
		c_current_cdemo_sk = cd2.cd_demo_sk AND
		c_current_hdemo_sk = hd2.hd_demo_sk AND
		c_current_addr_sk = ad2.ca_address_sk and
		c_first_sales_date_sk = d2.d_date_sk and
		c_first_shipto_date_sk = d3.d_date_sk and
		ss_promo_sk = p_promo_sk and
		hd1.hd_income_band_sk = ib1.ib_income_band_sk and
		hd2.hd_income_band_sk = ib2.ib_income_band_sk and
		cd1.cd_marital_status <> cd2.cd_marital_status and
		i_color in ('orange','lace','lawn','misty','blush','pink') and
		i_current_price between 48 and 48 + 10 and
		i_current_price between 48 + 1 and 48 + 15
		group by i_product_name
		,i_item_sk
		,s_store_name
		,s_zip
		,ad1.ca_street_number
		,ad1.ca_street_name
		,ad1.ca_city
		,ad1.ca_zip
		,ad2.ca_street_number
		,ad2.ca_street_name
		,ad2.ca_city
		,ad2.ca_zip
		,d1.d_year
		,d2.d_year
		,d3.d_year
	)
		select cs1.product_name
		,cs1.store_name
		,cs1.store_zip
		,cs1.b_street_number
		,cs1.b_street_name
		,cs1.b_city
		,cs1.b_zip
		,cs1.c_street_number
		,cs1.c_street_name
		,cs1.c_city
		,cs1.c_zip
		,cs1.syear
		,cs1.cnt
		,cs1.s1 as s11
		,cs1.s2 as s21
		,cs1.s3 as s31
		,cs2.s1 as s12
		,cs2.s2 as s22
		,cs2.s3 as s32
		,cs2.syear
		,cs2.cnt
		from cross_sales cs1,cross_sales cs2
		where cs1.item_sk=cs2.item_sk and
		cs1.syear = 1999 and
		cs2.syear = 1999 + 1 and
		cs2.cnt <= cs1.cnt and
		cs1.store_name = cs2.store_name and
		cs1.store_zip = cs2.store_zip
		order by cs1.product_name
		,cs1.store_name
		,cs2.cnt
		,cs1.s1
		,cs2.s1;`,
	},
	{
		Query: `
-- start query 65 in stream 0 using template query65.tpl
select 
	s_store_name,
	i_item_desc,
	sc.revenue,
	i_current_price,
	i_wholesale_cost,
	i_brand
 from store, item,
     (select ss_store_sk, avg(revenue) as ave
 	from
 	    (select  ss_store_sk, ss_item_sk, 
 		     sum(ss_sales_price) as revenue
 		from store_sales, date_dim
 		where ss_sold_date_sk = d_date_sk and d_month_seq between 1176 and 1176+11
 		group by ss_store_sk, ss_item_sk) sa
 	group by ss_store_sk) sb,
     (select  ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
 	from store_sales, date_dim
 	where ss_sold_date_sk = d_date_sk and d_month_seq between 1176 and 1176+11
 	group by ss_store_sk, ss_item_sk) sc
 where sb.ss_store_sk = sc.ss_store_sk and 
       sc.revenue <= 0.1 * sb.ave and
       s_store_sk = sc.ss_store_sk and
       i_item_sk = sc.ss_item_sk
 order by s_store_name, i_item_desc
limit 100;`,
	},
	{
		Query: `
		-- start query 66 in stream 0 using template query66.tpl
		select
		w_warehouse_name
		,w_warehouse_sq_ft
		,w_city
		,w_county
		,w_state
		,w_country
		,ship_carriers
		,year
		,sum(jan_sales) as jan_sales
		,sum(feb_sales) as feb_sales
		,sum(mar_sales) as mar_sales
		,sum(apr_sales) as apr_sales
		,sum(may_sales) as may_sales
		,sum(jun_sales) as jun_sales
		,sum(jul_sales) as jul_sales
		,sum(aug_sales) as aug_sales
		,sum(sep_sales) as sep_sales
		,sum(oct_sales) as oct_sales
		,sum(nov_sales) as nov_sales
		,sum(dec_sales) as dec_sales
		,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
		,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
		,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
		,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
		,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
		,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
		,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
		,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
		,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
		,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
		,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
		,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
		,sum(jan_net) as jan_net
		,sum(feb_net) as feb_net
		,sum(mar_net) as mar_net
		,sum(apr_net) as apr_net
		,sum(may_net) as may_net
		,sum(jun_net) as jun_net
		,sum(jul_net) as jul_net
		,sum(aug_net) as aug_net
		,sum(sep_net) as sep_net
		,sum(oct_net) as oct_net
		,sum(nov_net) as nov_net
		,sum(dec_net) as dec_net
		from (
		select
		w_warehouse_name
		,w_warehouse_sq_ft
		,w_city
		,w_county
		,w_state
		,w_country
		,'ORIENTAL' || ',' || 'BOXBUNDLES' as ship_carriers
		,d_year as year
		,sum(case when d_moy = 1
		then ws_ext_sales_price* ws_quantity else 0 end) as jan_sales
		,sum(case when d_moy = 2
		then ws_ext_sales_price* ws_quantity else 0 end) as feb_sales
		,sum(case when d_moy = 3
		then ws_ext_sales_price* ws_quantity else 0 end) as mar_sales
		,sum(case when d_moy = 4
		then ws_ext_sales_price* ws_quantity else 0 end) as apr_sales
		,sum(case when d_moy = 5
		then ws_ext_sales_price* ws_quantity else 0 end) as may_sales
		,sum(case when d_moy = 6
		then ws_ext_sales_price* ws_quantity else 0 end) as jun_sales
		,sum(case when d_moy = 7
		then ws_ext_sales_price* ws_quantity else 0 end) as jul_sales
		,sum(case when d_moy = 8
		then ws_ext_sales_price* ws_quantity else 0 end) as aug_sales
		,sum(case when d_moy = 9
		then ws_ext_sales_price* ws_quantity else 0 end) as sep_sales
		,sum(case when d_moy = 10
		then ws_ext_sales_price* ws_quantity else 0 end) as oct_sales
		,sum(case when d_moy = 11
		then ws_ext_sales_price* ws_quantity else 0 end) as nov_sales
		,sum(case when d_moy = 12
		then ws_ext_sales_price* ws_quantity else 0 end) as dec_sales
		,sum(case when d_moy = 1
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as jan_net
		,sum(case when d_moy = 2
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as feb_net
		,sum(case when d_moy = 3
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as mar_net
		,sum(case when d_moy = 4
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as apr_net
		,sum(case when d_moy = 5
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as may_net
		,sum(case when d_moy = 6
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as jun_net
		,sum(case when d_moy = 7
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as jul_net
		,sum(case when d_moy = 8
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as aug_net
		,sum(case when d_moy = 9
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as sep_net
		,sum(case when d_moy = 10
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as oct_net
		,sum(case when d_moy = 11
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as nov_net
		,sum(case when d_moy = 12
		then ws_net_paid_inc_ship * ws_quantity else 0 end) as dec_net
		from
		web_sales
		,warehouse
		,date_dim
		,time_dim
		,ship_mode
		where
		ws_warehouse_sk =  w_warehouse_sk
		and ws_sold_date_sk = d_date_sk
		and ws_sold_time_sk = t_time_sk
		and ws_ship_mode_sk = sm_ship_mode_sk
		and d_year = 2001
		and t_time between 42970 and 42970+28800
		and sm_carrier in ('ORIENTAL','BOXBUNDLES')
		group by
		w_warehouse_name
		,w_warehouse_sq_ft
		,w_city
		,w_county
		,w_state
		,w_country
		,d_year
		union all
		select
		w_warehouse_name
		,w_warehouse_sq_ft
		,w_city
		,w_county
		,w_state
		,w_country
		,'ORIENTAL' || ',' || 'BOXBUNDLES' as ship_carriers
		,d_year as year
		,sum(case when d_moy = 1
		then cs_ext_list_price* cs_quantity else 0 end) as jan_sales
		,sum(case when d_moy = 2
		then cs_ext_list_price* cs_quantity else 0 end) as feb_sales
		,sum(case when d_moy = 3
		then cs_ext_list_price* cs_quantity else 0 end) as mar_sales
		,sum(case when d_moy = 4
		then cs_ext_list_price* cs_quantity else 0 end) as apr_sales
		,sum(case when d_moy = 5
		then cs_ext_list_price* cs_quantity else 0 end) as may_sales
		,sum(case when d_moy = 6
		then cs_ext_list_price* cs_quantity else 0 end) as jun_sales
		,sum(case when d_moy = 7
		then cs_ext_list_price* cs_quantity else 0 end) as jul_sales
		,sum(case when d_moy = 8
		then cs_ext_list_price* cs_quantity else 0 end) as aug_sales
		,sum(case when d_moy = 9
		then cs_ext_list_price* cs_quantity else 0 end) as sep_sales
		,sum(case when d_moy = 10
		then cs_ext_list_price* cs_quantity else 0 end) as oct_sales
		,sum(case when d_moy = 11
		then cs_ext_list_price* cs_quantity else 0 end) as nov_sales
		,sum(case when d_moy = 12
		then cs_ext_list_price* cs_quantity else 0 end) as dec_sales
		,sum(case when d_moy = 1
		then cs_net_paid * cs_quantity else 0 end) as jan_net
		,sum(case when d_moy = 2
		then cs_net_paid * cs_quantity else 0 end) as feb_net
		,sum(case when d_moy = 3
		then cs_net_paid * cs_quantity else 0 end) as mar_net
		,sum(case when d_moy = 4
		then cs_net_paid * cs_quantity else 0 end) as apr_net
		,sum(case when d_moy = 5
		then cs_net_paid * cs_quantity else 0 end) as may_net
		,sum(case when d_moy = 6
		then cs_net_paid * cs_quantity else 0 end) as jun_net
		,sum(case when d_moy = 7
		then cs_net_paid * cs_quantity else 0 end) as jul_net
		,sum(case when d_moy = 8
		then cs_net_paid * cs_quantity else 0 end) as aug_net
		,sum(case when d_moy = 9
		then cs_net_paid * cs_quantity else 0 end) as sep_net
		,sum(case when d_moy = 10
		then cs_net_paid * cs_quantity else 0 end) as oct_net
		,sum(case when d_moy = 11
		then cs_net_paid * cs_quantity else 0 end) as nov_net
		,sum(case when d_moy = 12
		then cs_net_paid * cs_quantity else 0 end) as dec_net
		from
		catalog_sales
		,warehouse
		,date_dim
		,time_dim
		,ship_mode
		where
		cs_warehouse_sk =  w_warehouse_sk
		and cs_sold_date_sk = d_date_sk
		and cs_sold_time_sk = t_time_sk
		and cs_ship_mode_sk = sm_ship_mode_sk
		and d_year = 2001
		and t_time between 42970 AND 42970+28800
		and sm_carrier in ('ORIENTAL','BOXBUNDLES')
		group by
		w_warehouse_name
		,w_warehouse_sq_ft
		,w_city
		,w_county
		,w_state
		,w_country
		,d_year
	) x
		group by
		w_warehouse_name
		,w_warehouse_sq_ft
		,w_city
		,w_county
		,w_state
		,w_country
		,ship_carriers
		,year
		order by w_warehouse_name
		limit 100;`,
	},
	{
		Query: `
-- start query 67 in stream 0 using template query67.tpl
select  *
from (select i_category
            ,i_class
            ,i_brand
            ,i_product_name
            ,d_year
            ,d_qoy
            ,d_moy
            ,s_store_id
            ,sumsales
            ,rank() over (partition by i_category order by sumsales desc) rk
      from (select i_category
                  ,i_class
                  ,i_brand
                  ,i_product_name
                  ,d_year
                  ,d_qoy
                  ,d_moy
                  ,s_store_id
                  ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
            from store_sales
                ,date_dim
                ,store
                ,item
       where  ss_sold_date_sk=d_date_sk
          and ss_item_sk=i_item_sk
          and ss_store_sk = s_store_sk
          and d_month_seq between 1217 and 1217+11
       group by  rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
where rk <= 100
order by i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
limit 100;`,
		Skip: true,
	},
	{
		Query: `
		-- start query 68 in stream 0 using template query68.tpl
		select  c_last_name
		,c_first_name
		,ca_city
		,bought_city
		,ss_ticket_number
		,extended_price
		,extended_tax
		,list_price
		from (select ss_ticket_number
		,ss_customer_sk
		,ca_city bought_city
		,sum(ss_ext_sales_price) extended_price
		,sum(ss_ext_list_price) list_price
		,sum(ss_ext_tax) extended_tax
		from store_sales
		,date_dim
		,store
		,household_demographics
		,customer_address
		where store_sales.ss_sold_date_sk = date_dim.d_date_sk
		and store_sales.ss_store_sk = store.s_store_sk
		and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
		and store_sales.ss_addr_sk = customer_address.ca_address_sk
		and date_dim.d_dom between 1 and 2
		and (household_demographics.hd_dep_count = 3 or
		household_demographics.hd_vehicle_count= 4)
		and date_dim.d_year in (1998,1998+1,1998+2)
		and store.s_city in ('Fairview','Midway')
		group by ss_ticket_number
		,ss_customer_sk
		,ss_addr_sk,ca_city) dn
		,customer
		,customer_address current_addr
		where ss_customer_sk = c_customer_sk
		and customer.c_current_addr_sk = current_addr.ca_address_sk
		and current_addr.ca_city <> bought_city
		order by c_last_name
		,ss_ticket_number
		limit 100;`,
	},
	{
		Query: `
-- start query 69 in stream 0 using template query69.tpl
select  
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('IL','TX','ME') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2002 and
                d_moy between 1 and 1+2) and
   (not exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2002 and
                  d_moy between 1 and 1+2) and
    not exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2002 and
                  d_moy between 1 and 1+2))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 limit 100;`,
	},
	{
		Query: `
		-- start query 70 in stream 0 using template query70.tpl
		select
		sum(ss_net_profit) as total_sum
		,s_state
		,s_county
		,grouping(s_state)+grouping(s_county) as lochierarchy
		,rank() over (
		partition by grouping(s_state)+grouping(s_county),
	case when grouping(s_county) = 0 then s_state end
		order by sum(ss_net_profit) desc) as rank_within_parent
		from
		store_sales
		,date_dim       d1
		,store
		where
		d1.d_month_seq between 1220 and 1220+11
		and d1.d_date_sk = ss_sold_date_sk
		and s_store_sk  = ss_store_sk
		and s_state in
	( select s_state
		from  (select s_state as s_state,
		rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
		from   store_sales, store, date_dim
		where  d_month_seq between 1220 and 1220+11
		and d_date_sk = ss_sold_date_sk
		and s_store_sk  = ss_store_sk
		group by s_state
	) tmp1
		where ranking <= 5
	)
		group by rollup(s_state,s_county)
		order by
		lochierarchy desc
		,case when lochierarchy = 0 then s_state end
		,rank_within_parent
		limit 100;`,
		Skip: true,
	},
	{
		Query: `
-- start query 71 in stream 0 using template query71.tpl
select i_brand_id brand_id, i_brand brand,t_hour,t_minute,
 	sum(ext_price) ext_price
 from item, (select ws_ext_sales_price as ext_price, 
                        ws_sold_date_sk as sold_date_sk,
                        ws_item_sk as sold_item_sk,
                        ws_sold_time_sk as time_sk  
                 from web_sales,date_dim
                 where d_date_sk = ws_sold_date_sk
                   and d_moy=12
                   and d_year=2002
                 union all
                 select cs_ext_sales_price as ext_price,
                        cs_sold_date_sk as sold_date_sk,
                        cs_item_sk as sold_item_sk,
                        cs_sold_time_sk as time_sk
                 from catalog_sales,date_dim
                 where d_date_sk = cs_sold_date_sk
                   and d_moy=12
                   and d_year=2002
                 union all
                 select ss_ext_sales_price as ext_price,
                        ss_sold_date_sk as sold_date_sk,
                        ss_item_sk as sold_item_sk,
                        ss_sold_time_sk as time_sk
                 from store_sales,date_dim
                 where d_date_sk = ss_sold_date_sk
                   and d_moy=12
                   and d_year=2002
                 ) tmp,time_dim
 where
   sold_item_sk = i_item_sk
   and i_manager_id=1
   and time_sk = t_time_sk
   and (t_meal_time = 'breakfast' or t_meal_time = 'dinner')
 group by i_brand, i_brand_id,t_hour,t_minute
 order by ext_price desc, i_brand_id
 ;`,
	},
	{
		Query: `
		-- start query 72 in stream 0 using template query72.tpl
		select  i_item_desc
		,w_warehouse_name
		,d1.d_week_seq
		,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
		,sum(case when p_promo_sk is not null then 1 else 0 end) promo
		,count(*) total_cnt
		from catalog_sales
		join inventory on (cs_item_sk = inv_item_sk)
		join warehouse on (w_warehouse_sk=inv_warehouse_sk)
		join item on (i_item_sk = cs_item_sk)
		join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
		join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
		join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
		join date_dim d2 on (inv_date_sk = d2.d_date_sk)
		join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
		left outer join promotion on (cs_promo_sk=p_promo_sk)
		left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
		where d1.d_week_seq = d2.d_week_seq
		and inv_quantity_on_hand < cs_quantity
		and d3.d_date > d1.d_date + 5
		and hd_buy_potential = '1001-5000'
		and d1.d_year = 1998
		and cd_marital_status = 'S'
		group by i_item_desc,w_warehouse_name,d1.d_week_seq
		order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
		limit 100;`,
	},
	{
		Query: `
-- start query 73 in stream 0 using template query73.tpl
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag 
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and date_dim.d_dom between 1 and 2 
    and (household_demographics.hd_buy_potential = '1001-5000' or
         household_demographics.hd_buy_potential = '5001-10000')
    and household_demographics.hd_vehicle_count > 0
    and case when household_demographics.hd_vehicle_count > 0 then 
             household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count else null end > 1
    and date_dim.d_year in (2000,2000+1,2000+2)
    and store.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County')
    group by ss_ticket_number,ss_customer_sk) dj,customer
    where ss_customer_sk = c_customer_sk
      and cnt between 1 and 5
    order by cnt desc, c_last_name asc;`,
	},
	{
		Query: `
		-- start query 74 in stream 0 using template query74.tpl
		with year_total as (
		select c_customer_id customer_id
		,c_first_name customer_first_name
		,c_last_name customer_last_name
		,d_year as year
		,max(ss_net_paid) year_total
		,'s' sale_type
		from customer
		,store_sales
		,date_dim
		where c_customer_sk = ss_customer_sk
		and ss_sold_date_sk = d_date_sk
		and d_year in (1999,1999+1)
		group by c_customer_id
		,c_first_name
		,c_last_name
		,d_year
		union all
		select c_customer_id customer_id
		,c_first_name customer_first_name
		,c_last_name customer_last_name
		,d_year as year
		,max(ws_net_paid) year_total
		,'w' sale_type
		from customer
		,web_sales
		,date_dim
		where c_customer_sk = ws_bill_customer_sk
		and ws_sold_date_sk = d_date_sk
		and d_year in (1999,1999+1)
		group by c_customer_id
		,c_first_name
		,c_last_name
		,d_year
	)
		select
		t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
		from year_total t_s_firstyear
		,year_total t_s_secyear
		,year_total t_w_firstyear
		,year_total t_w_secyear
		where t_s_secyear.customer_id = t_s_firstyear.customer_id
		and t_s_firstyear.customer_id = t_w_secyear.customer_id
		and t_s_firstyear.customer_id = t_w_firstyear.customer_id
		and t_s_firstyear.sale_type = 's'
		and t_w_firstyear.sale_type = 'w'
		and t_s_secyear.sale_type = 's'
		and t_w_secyear.sale_type = 'w'
		and t_s_firstyear.year = 1999
		and t_s_secyear.year = 1999+1
		and t_w_firstyear.year = 1999
		and t_w_secyear.year = 1999+1
		and t_s_firstyear.year_total > 0
		and t_w_firstyear.year_total > 0
		and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
		> case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
		order by 1,3,2
		limit 100;`,
	},
	{
		Query: `
-- start query 75 in stream 0 using template query75.tpl
WITH all_sales AS (
 SELECT d_year
       ,i_brand_id
       ,i_class_id
       ,i_category_id
       ,i_manufact_id
       ,SUM(sales_cnt) AS sales_cnt
       ,SUM(sales_amt) AS sales_amt
 FROM (SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt
             ,cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
       FROM catalog_sales JOIN item ON i_item_sk=cs_item_sk
                          JOIN date_dim ON d_date_sk=cs_sold_date_sk
                          LEFT JOIN catalog_returns ON (cs_order_number=cr_order_number 
                                                    AND cs_item_sk=cr_item_sk)
       WHERE i_category='Sports'
       UNION
       SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt
             ,ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
       FROM store_sales JOIN item ON i_item_sk=ss_item_sk
                        JOIN date_dim ON d_date_sk=ss_sold_date_sk
                        LEFT JOIN store_returns ON (ss_ticket_number=sr_ticket_number 
                                                AND ss_item_sk=sr_item_sk)
       WHERE i_category='Sports'
       UNION
       SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt
             ,ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt
       FROM web_sales JOIN item ON i_item_sk=ws_item_sk
                      JOIN date_dim ON d_date_sk=ws_sold_date_sk
                      LEFT JOIN web_returns ON (ws_order_number=wr_order_number 
                                            AND ws_item_sk=wr_item_sk)
       WHERE i_category='Sports') sales_detail
 GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id)
 SELECT  prev_yr.d_year AS prev_year
                          ,curr_yr.d_year AS year
                          ,curr_yr.i_brand_id
                          ,curr_yr.i_class_id
                          ,curr_yr.i_category_id
                          ,curr_yr.i_manufact_id
                          ,prev_yr.sales_cnt AS prev_yr_cnt
                          ,curr_yr.sales_cnt AS curr_yr_cnt
                          ,curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff
                          ,curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
 FROM all_sales curr_yr, all_sales prev_yr
 WHERE curr_yr.i_brand_id=prev_yr.i_brand_id
   AND curr_yr.i_class_id=prev_yr.i_class_id
   AND curr_yr.i_category_id=prev_yr.i_category_id
   AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
   AND curr_yr.d_year=2002
   AND prev_yr.d_year=2002-1
   AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))/CAST(prev_yr.sales_cnt AS DECIMAL(17,2))<0.9
 ORDER BY sales_cnt_diff,sales_amt_diff
 limit 100;`,
	},
	{
		Query: `
		-- start query 76 in stream 0 using template query76.tpl
		select  channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
		SELECT 'store' as channel, 'ss_customer_sk' col_name, d_year, d_qoy, i_category, ss_ext_sales_price ext_sales_price
		FROM store_sales, item, date_dim
		WHERE ss_customer_sk IS NULL
		AND ss_sold_date_sk=d_date_sk
		AND ss_item_sk=i_item_sk
		UNION ALL
		SELECT 'web' as channel, 'ws_promo_sk' col_name, d_year, d_qoy, i_category, ws_ext_sales_price ext_sales_price
		FROM web_sales, item, date_dim
		WHERE ws_promo_sk IS NULL
		AND ws_sold_date_sk=d_date_sk
		AND ws_item_sk=i_item_sk
		UNION ALL
		SELECT 'catalog' as channel, 'cs_bill_customer_sk' col_name, d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price
		FROM catalog_sales, item, date_dim
		WHERE cs_bill_customer_sk IS NULL
		AND cs_sold_date_sk=d_date_sk
		AND cs_item_sk=i_item_sk) foo
		GROUP BY channel, col_name, d_year, d_qoy, i_category
		ORDER BY channel, col_name, d_year, d_qoy, i_category
		limit 100;`,
	},
	{
		Query: `
-- start query 77 in stream 0 using template query77.tpl
with ss as
 (select s_store_sk,
         sum(ss_ext_sales_price) as sales,
         sum(ss_net_profit) as profit
 from store_sales,
      date_dim,
      store
 where ss_sold_date_sk = d_date_sk
       and d_date between cast('2000-08-10' as date) 
                  and (cast('2000-08-10' as date) +  interval 30 day) 
       and ss_store_sk = s_store_sk
 group by s_store_sk)
 ,
 sr as
 (select s_store_sk,
         sum(sr_return_amt) as returns,
         sum(sr_net_loss) as profit_loss
 from store_returns,
      date_dim,
      store
 where sr_returned_date_sk = d_date_sk
       and d_date between cast('2000-08-10' as date)
                  and (cast('2000-08-10' as date) +  interval 30 day)
       and sr_store_sk = s_store_sk
 group by s_store_sk), 
 cs as
 (select cs_call_center_sk,
        sum(cs_ext_sales_price) as sales,
        sum(cs_net_profit) as profit
 from catalog_sales,
      date_dim
 where cs_sold_date_sk = d_date_sk
       and d_date between cast('2000-08-10' as date)
                  and (cast('2000-08-10' as date) +  interval 30 day)
 group by cs_call_center_sk 
 ), 
 cr as
 (select cr_call_center_sk,
         sum(cr_return_amount) as returns,
         sum(cr_net_loss) as profit_loss
 from catalog_returns,
      date_dim
 where cr_returned_date_sk = d_date_sk
       and d_date between cast('2000-08-10' as date)
                  and (cast('2000-08-10' as date) +  interval 30 day)
 group by cr_call_center_sk
 ), 
 ws as
 ( select wp_web_page_sk,
        sum(ws_ext_sales_price) as sales,
        sum(ws_net_profit) as profit
 from web_sales,
      date_dim,
      web_page
 where ws_sold_date_sk = d_date_sk
       and d_date between cast('2000-08-10' as date)
                  and (cast('2000-08-10' as date) +  interval 30 day)
       and ws_web_page_sk = wp_web_page_sk
 group by wp_web_page_sk), 
 wr as
 (select wp_web_page_sk,
        sum(wr_return_amt) as returns,
        sum(wr_net_loss) as profit_loss
 from web_returns,
      date_dim,
      web_page
 where wr_returned_date_sk = d_date_sk
       and d_date between cast('2000-08-10' as date)
                  and (cast('2000-08-10' as date) +  interval 30 day)
       and wr_web_page_sk = wp_web_page_sk
 group by wp_web_page_sk)
  select  channel
        , id
        , sum(sales) as sales
        , sum(returns) as returns
        , sum(profit) as profit
 from 
 (select 'store channel' as channel
        , ss.s_store_sk as id
        , sales
        , coalesce(returns, 0) as returns
        , (profit - coalesce(profit_loss,0)) as profit
 from   ss left join sr
        on  ss.s_store_sk = sr.s_store_sk
 union all
 select 'catalog channel' as channel
        , cs_call_center_sk as id
        , sales
        , returns
        , (profit - profit_loss) as profit
 from  cs
       , cr
 union all
 select 'web channel' as channel
        , ws.wp_web_page_sk as id
        , sales
        , coalesce(returns, 0) returns
        , (profit - coalesce(profit_loss,0)) as profit
 from   ws left join wr
        on  ws.wp_web_page_sk = wr.wp_web_page_sk
 ) x
 group by rollup (channel, id)
 order by channel
         ,id
 limit 100;`,
		Skip: true,
	},
	{
		Query: `
		-- start query 78 in stream 0 using template query78.tpl
		with ws as
	(select d_year AS ws_sold_year, ws_item_sk,
		ws_bill_customer_sk ws_customer_sk,
		sum(ws_quantity) ws_qty,
		sum(ws_wholesale_cost) ws_wc,
		sum(ws_sales_price) ws_sp
		from web_sales
		left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk
		join date_dim on ws_sold_date_sk = d_date_sk
		where wr_order_number is null
		group by d_year, ws_item_sk, ws_bill_customer_sk
	),
		cs as
	(select d_year AS cs_sold_year, cs_item_sk,
		cs_bill_customer_sk cs_customer_sk,
		sum(cs_quantity) cs_qty,
		sum(cs_wholesale_cost) cs_wc,
		sum(cs_sales_price) cs_sp
		from catalog_sales
		left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk
		join date_dim on cs_sold_date_sk = d_date_sk
		where cr_order_number is null
		group by d_year, cs_item_sk, cs_bill_customer_sk
	),
		ss as
	(select d_year AS ss_sold_year, ss_item_sk,
		ss_customer_sk,
		sum(ss_quantity) ss_qty,
		sum(ss_wholesale_cost) ss_wc,
		sum(ss_sales_price) ss_sp
		from store_sales
		left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk
		join date_dim on ss_sold_date_sk = d_date_sk
		where sr_ticket_number is null
		group by d_year, ss_item_sk, ss_customer_sk
	)
		select
		ss_customer_sk,
		round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
		ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
		coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
		coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,
		coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price
		from ss
		left join ws on (ws_sold_year=ss_sold_year and ws_item_sk=ss_item_sk and ws_customer_sk=ss_customer_sk)
		left join cs on (cs_sold_year=ss_sold_year and cs_item_sk=ss_item_sk and cs_customer_sk=ss_customer_sk)
		where (coalesce(ws_qty,0)>0 or coalesce(cs_qty, 0)>0) and ss_sold_year=1998
		order by
		ss_customer_sk,
		ss_qty desc, ss_wc desc, ss_sp desc,
		other_chan_qty,
		other_chan_wholesale_cost,
		other_chan_sales_price,
		ratio
		limit 100;`,
	},
	{
		Query: `
-- start query 79 in stream 0 using template query79.tpl
select 
  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,store.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (household_demographics.hd_dep_count = 7 or household_demographics.hd_vehicle_count > -1)
    and date_dim.d_dow = 1
    and date_dim.d_year in (2000,2000+1,2000+2) 
    and store.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store.s_city) ms,customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substr(s_city,1,30), profit
limit 100;`,
	},
	{
		Query: `
		-- start query 80 in stream 0 using template query80.tpl
		with ssr as
	(select  s_store_id as store_id,
		sum(ss_ext_sales_price) as sales,
		sum(coalesce(sr_return_amt, 0)) as returns,
		sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
		from store_sales left outer join store_returns on
	(ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
		date_dim,
		store,
		item,
		promotion
		where ss_sold_date_sk = d_date_sk
		and d_date between cast('2002-08-14' as date)
		and (cast('2002-08-14' as date) +  interval 30 day)
		and ss_store_sk = s_store_sk
		and ss_item_sk = i_item_sk
		and i_current_price > 50
		and ss_promo_sk = p_promo_sk
		and p_channel_tv = 'N'
		group by s_store_id)
		,
		csr as
	(select  cp_catalog_page_id as catalog_page_id,
		sum(cs_ext_sales_price) as sales,
		sum(coalesce(cr_return_amount, 0)) as returns,
		sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
		from catalog_sales left outer join catalog_returns on
	(cs_item_sk = cr_item_sk and cs_order_number = cr_order_number),
		date_dim,
		catalog_page,
		item,
		promotion
		where cs_sold_date_sk = d_date_sk
		and d_date between cast('2002-08-14' as date)
		and (cast('2002-08-14' as date) +  interval 30 day)
		and cs_catalog_page_sk = cp_catalog_page_sk
		and cs_item_sk = i_item_sk
		and i_current_price > 50
		and cs_promo_sk = p_promo_sk
		and p_channel_tv = 'N'
		group by cp_catalog_page_id)
		,
		wsr as
	(select  web_site_id,
		sum(ws_ext_sales_price) as sales,
		sum(coalesce(wr_return_amt, 0)) as returns,
		sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
		from web_sales left outer join web_returns on
	(ws_item_sk = wr_item_sk and ws_order_number = wr_order_number),
		date_dim,
		web_site,
		item,
		promotion
		where ws_sold_date_sk = d_date_sk
		and d_date between cast('2002-08-14' as date)
		and (cast('2002-08-14' as date) +  interval 30 day)
		and ws_web_site_sk = web_site_sk
		and ws_item_sk = i_item_sk
		and i_current_price > 50
		and ws_promo_sk = p_promo_sk
		and p_channel_tv = 'N'
		group by web_site_id)
		select  channel
		, id
		, sum(sales) as sales
		, sum(returns) as returns
		, sum(profit) as profit
		from
	(select 'store channel' as channel
		, 'store' || store_id as id
		, sales
		, returns
		, profit
		from   ssr
		union all
		select 'catalog channel' as channel
		, 'catalog_page' || catalog_page_id as id
		, sales
		, returns
		, profit
		from  csr
		union all
		select 'web channel' as channel
		, 'web_site' || web_site_id as id
		, sales
		, returns
		, profit
		from   wsr
	) x
		group by rollup (channel, id)
		order by channel
		,id
		limit 100;`,
		Skip: true,
	},
	{
		Query: `
-- start query 81 in stream 0 using template query81.tpl
with customer_total_return as
 (select cr_returning_customer_sk as ctr_customer_sk
        ,ca_state as ctr_state, 
 	sum(cr_return_amt_inc_tax) as ctr_total_return
 from catalog_returns
     ,date_dim
     ,customer_address
 where cr_returned_date_sk = d_date_sk 
   and d_year =2001
   and cr_returning_addr_sk = ca_address_sk 
 group by cr_returning_customer_sk
         ,ca_state )
  select  c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 from customer_total_return ctr1
     ,customer_address
     ,customer
 where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 			  from customer_total_return ctr2 
                  	  where ctr1.ctr_state = ctr2.ctr_state)
       and ca_address_sk = c_current_addr_sk
       and ca_state = 'TN'
       and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 limit 100;`,
	},
	{
		Query: `
		-- start query 82 in stream 0 using template query82.tpl
		select  i_item_id
		,i_item_desc
		,i_current_price
		from item, inventory, date_dim, store_sales
		where i_current_price between 58 and 58+30
		and inv_item_sk = i_item_sk
		and d_date_sk=inv_date_sk
		and d_date between cast('2001-01-13' as date) and (cast('2001-01-13' as date) +  interval 60 day)
		and i_manufact_id in (259,559,580,485)
		and inv_quantity_on_hand between 100 and 500
		and ss_item_sk = i_item_sk
		group by i_item_id,i_item_desc,i_current_price
		order by i_item_id
		limit 100;`,
	},
	{
		Query: `
-- start query 83 in stream 0 using template query83.tpl
with sr_items as
 (select i_item_id item_id,
        sum(sr_return_quantity) sr_item_qty
 from store_returns,
      item,
      date_dim
 where sr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
	  where d_date in ('2001-07-13','2001-09-10','2001-11-16')))
 and   sr_returned_date_sk   = d_date_sk
 group by i_item_id),
 cr_items as
 (select i_item_id item_id,
        sum(cr_return_quantity) cr_item_qty
 from catalog_returns,
      item,
      date_dim
 where cr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
	  where d_date in ('2001-07-13','2001-09-10','2001-11-16')))
 and   cr_returned_date_sk   = d_date_sk
 group by i_item_id),
 wr_items as
 (select i_item_id item_id,
        sum(wr_return_quantity) wr_item_qty
 from web_returns,
      item,
      date_dim
 where wr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
		where d_date in ('2001-07-13','2001-09-10','2001-11-16')))
 and   wr_returned_date_sk   = d_date_sk
 group by i_item_id)
  select  sr_items.item_id
       ,sr_item_qty
       ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev
       ,cr_item_qty
       ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev
       ,wr_item_qty
       ,wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev
       ,(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 average
 from sr_items
     ,cr_items
     ,wr_items
 where sr_items.item_id=cr_items.item_id
   and sr_items.item_id=wr_items.item_id 
 order by sr_items.item_id
         ,sr_item_qty
 limit 100;`,
	},
	{
		Query: `
		-- start query 84 in stream 0 using template query84.tpl
		select  c_customer_id as customer_id
		, coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
		from customer
		,customer_address
		,customer_demographics
		,household_demographics
		,income_band
		,store_returns
		where ca_city	        =  'Woodland'
		and c_current_addr_sk = ca_address_sk
		and ib_lower_bound   >=  60306
		and ib_upper_bound   <=  60306 + 50000
		and ib_income_band_sk = hd_income_band_sk
		and cd_demo_sk = c_current_cdemo_sk
		and hd_demo_sk = c_current_hdemo_sk
		and sr_cdemo_sk = cd_demo_sk
		order by c_customer_id
		limit 100;`,
	},
	{
		Query: `
-- start query 85 in stream 0 using template query85.tpl
select  substr(r_reason_desc,1,20)
       ,avg(ws_quantity)
       ,avg(wr_refunded_cash)
       ,avg(wr_fee)
 from web_sales, web_returns, web_page, customer_demographics cd1,
      customer_demographics cd2, customer_address, date_dim, reason 
 where ws_web_page_sk = wp_web_page_sk
   and ws_item_sk = wr_item_sk
   and ws_order_number = wr_order_number
   and ws_sold_date_sk = d_date_sk and d_year = 1998
   and cd1.cd_demo_sk = wr_refunded_cdemo_sk 
   and cd2.cd_demo_sk = wr_returning_cdemo_sk
   and ca_address_sk = wr_refunded_addr_sk
   and r_reason_sk = wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = 'D'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Primary'
     and 
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 100.00 and 150.00
    )
   or
    (
     cd1.cd_marital_status = 'S'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'College' 
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 50.00 and 100.00
    )
   or
    (
     cd1.cd_marital_status = 'U'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Advanced Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 150.00 and 200.00
    )
   )
   and
   (
    (
     ca_country = 'United States'
     and
     ca_state in ('NC', 'TX', 'IA')
     and ws_net_profit between 100 and 200  
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('WI', 'WV', 'GA')
     and ws_net_profit between 150 and 300  
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('OK', 'VA', 'KY')
     and ws_net_profit between 50 and 250  
    )
   )
group by r_reason_desc
order by substr(r_reason_desc,1,20)
        ,avg(ws_quantity)
        ,avg(wr_refunded_cash)
        ,avg(wr_fee)
limit 100;`,
	},
	{
		Query: `
		-- start query 86 in stream 0 using template query86.tpl
		select
		sum(ws_net_paid) as total_sum
		,i_category
		,i_class
		,grouping(i_category)+grouping(i_class) as lochierarchy
		,rank() over (
		partition by grouping(i_category)+grouping(i_class),
	case when grouping(i_class) = 0 then i_category end
		order by sum(ws_net_paid) desc) as rank_within_parent
		from
		web_sales
		,date_dim       d1
		,item
		where
		d1.d_month_seq between 1186 and 1186+11
		and d1.d_date_sk = ws_sold_date_sk
		and i_item_sk  = ws_item_sk
		group by rollup(i_category,i_class)
		order by
		lochierarchy desc,
	case when lochierarchy = 0 then i_category end,
		rank_within_parent
		limit 100;`,
		Skip: true,
	},
	{
		Query: `
-- start query 87 in stream 0 using template query87.tpl
select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from store_sales, date_dim, customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1202 and 1202+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from catalog_sales, date_dim, customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1202 and 1202+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from web_sales, date_dim, customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1202 and 1202+11)
) cool_cust
;`,
	},
	{
		Query: `
		-- start query 88 in stream 0 using template query88.tpl
		select  *
		from
	(select count(*) h8_30_to_9
		from store_sales, household_demographics , time_dim, store
		where ss_sold_time_sk = time_dim.t_time_sk
		and ss_hdemo_sk = household_demographics.hd_demo_sk
		and ss_store_sk = s_store_sk
		and time_dim.t_hour = 8
		and time_dim.t_minute >= 30
		and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
	(household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
	(household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
		and store.s_store_name = 'ese') s1,
	(select count(*) h9_to_9_30
		from store_sales, household_demographics , time_dim, store
		where ss_sold_time_sk = time_dim.t_time_sk
		and ss_hdemo_sk = household_demographics.hd_demo_sk
		and ss_store_sk = s_store_sk
		and time_dim.t_hour = 9
		and time_dim.t_minute < 30
		and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
	(household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
	(household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
		and store.s_store_name = 'ese') s2,
	(select count(*) h9_30_to_10
		from store_sales, household_demographics , time_dim, store
		where ss_sold_time_sk = time_dim.t_time_sk
		and ss_hdemo_sk = household_demographics.hd_demo_sk
		and ss_store_sk = s_store_sk
		and time_dim.t_hour = 9
		and time_dim.t_minute >= 30
		and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
	(household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
	(household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
		and store.s_store_name = 'ese') s3,
	(select count(*) h10_to_10_30
		from store_sales, household_demographics , time_dim, store
		where ss_sold_time_sk = time_dim.t_time_sk
		and ss_hdemo_sk = household_demographics.hd_demo_sk
		and ss_store_sk = s_store_sk
		and time_dim.t_hour = 10
		and time_dim.t_minute < 30
		and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
	(household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
	(household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
		and store.s_store_name = 'ese') s4,
	(select count(*) h10_30_to_11
		from store_sales, household_demographics , time_dim, store
		where ss_sold_time_sk = time_dim.t_time_sk
		and ss_hdemo_sk = household_demographics.hd_demo_sk
		and ss_store_sk = s_store_sk
		and time_dim.t_hour = 10
		and time_dim.t_minute >= 30
		and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
	(household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
	(household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
		and store.s_store_name = 'ese') s5,
	(select count(*) h11_to_11_30
		from store_sales, household_demographics , time_dim, store
		where ss_sold_time_sk = time_dim.t_time_sk
		and ss_hdemo_sk = household_demographics.hd_demo_sk
		and ss_store_sk = s_store_sk
		and time_dim.t_hour = 11
		and time_dim.t_minute < 30
		and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
	(household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
	(household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
		and store.s_store_name = 'ese') s6,
	(select count(*) h11_30_to_12
		from store_sales, household_demographics , time_dim, store
		where ss_sold_time_sk = time_dim.t_time_sk
		and ss_hdemo_sk = household_demographics.hd_demo_sk
		and ss_store_sk = s_store_sk
		and time_dim.t_hour = 11
		and time_dim.t_minute >= 30
		and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
	(household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
	(household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
		and store.s_store_name = 'ese') s7,
	(select count(*) h12_to_12_30
		from store_sales, household_demographics , time_dim, store
		where ss_sold_time_sk = time_dim.t_time_sk
		and ss_hdemo_sk = household_demographics.hd_demo_sk
		and ss_store_sk = s_store_sk
		and time_dim.t_hour = 12
		and time_dim.t_minute < 30
		and ((household_demographics.hd_dep_count = 0 and household_demographics.hd_vehicle_count<=0+2) or
	(household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
	(household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
		and store.s_store_name = 'ese') s8
		;`,
	},
	{
		Query: `
-- start query 89 in stream 0 using template query89.tpl
select  *
from(
select i_category, i_class, i_brand,
       s_store_name, s_company_name,
       d_moy,
       sum(ss_sales_price) sum_sales,
       avg(sum(ss_sales_price)) over
         (partition by i_category, i_brand, s_store_name, s_company_name)
         avg_monthly_sales
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
      ss_sold_date_sk = d_date_sk and
      ss_store_sk = s_store_sk and
      d_year in (2001) and
        ((i_category in ('Books','Children','Electronics') and
          i_class in ('history','school-uniforms','audio')
         )
      or (i_category in ('Men','Sports','Shoes') and
          i_class in ('pants','tennis','womens') 
        ))
group by i_category, i_class, i_brand,
         s_store_name, s_company_name, d_moy) tmp1
where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
order by sum_sales - avg_monthly_sales, s_store_name
limit 100;`,
	},
	{
		Query: `
		-- start query 90 in stream 0 using template query90.tpl
		select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
		from ( select count(*) amc
		from web_sales, household_demographics , time_dim, web_page
		where ws_sold_time_sk = time_dim.t_time_sk
		and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
		and ws_web_page_sk = web_page.wp_web_page_sk
		and time_dim.t_hour between 12 and 12+1
		and household_demographics.hd_dep_count = 6
		and web_page.wp_char_count between 5000 and 5200) at,
	( select count(*) pmc
		from web_sales, household_demographics , time_dim, web_page
		where ws_sold_time_sk = time_dim.t_time_sk
		and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
		and ws_web_page_sk = web_page.wp_web_page_sk
		and time_dim.t_hour between 14 and 14+1
		and household_demographics.hd_dep_count = 6
		and web_page.wp_char_count between 5000 and 5200) pt
		order by am_pm_ratio
		limit 100;`,
	},
	{
		Query: `
-- start query 91 in stream 0 using template query91.tpl
select  
        cc_call_center_id Call_Center,
        cc_name Call_Center_Name,
        cc_manager Manager,
        sum(cr_net_loss) Returns_Loss
from
        call_center,
        catalog_returns,
        date_dim,
        customer,
        customer_address,
        customer_demographics,
        household_demographics
where
        cr_call_center_sk       = cc_call_center_sk
and     cr_returned_date_sk     = d_date_sk
and     cr_returning_customer_sk= c_customer_sk
and     cd_demo_sk              = c_current_cdemo_sk
and     hd_demo_sk              = c_current_hdemo_sk
and     ca_address_sk           = c_current_addr_sk
and     d_year                  = 2000 
and     d_moy                   = 12
and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
        or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
and     hd_buy_potential like 'Unknown%'
and     ca_gmt_offset           = -7
group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
order by sum(cr_net_loss) desc;`,
	},
	{
		Query: `
		-- start query 92 in stream 0 using template query92.tpl
		select
		sum(ws_ext_discount_amt)  as "Excess Discount Amount"
		from
		web_sales
		,item
		,date_dim
		where
		i_manufact_id = 714
		and i_item_sk = ws_item_sk
		and d_date between '2000-02-01' and
	(cast('2000-02-01' as date) + interval 90 day)
		and d_date_sk = ws_sold_date_sk
		and ws_ext_discount_amt
		> (
		SELECT
		1.3 * avg(ws_ext_discount_amt)
		FROM
		web_sales
		,date_dim
		WHERE
		ws_item_sk = i_item_sk
		and d_date between '2000-02-01' and
	(cast('2000-02-01' as date) + interval 90 day)
		and d_date_sk = ws_sold_date_sk
	)
		order by sum(ws_ext_discount_amt)
		limit 100;`,
	},
	{
		Query: `
-- start query 93 in stream 0 using template query93.tpl
select  ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from store_sales left outer join store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number)
                ,reason
            where sr_reason_sk = r_reason_sk
              and r_reason_desc = 'reason 58') t
      group by ss_customer_sk
      order by sumsales, ss_customer_sk
limit 100;`,
	},
	{
		Query: `
		-- start query 94 in stream 0 using template query94.tpl
		select
		count(distinct ws_order_number) as "order count"
		,sum(ws_ext_ship_cost) as "total shipping cost"
		,sum(ws_net_profit) as "total net profit"
		from
		web_sales ws1
		,date_dim
		,customer_address
		,web_site
		where
		d_date between '2002-5-01' and
	(cast('2002-5-01' as date) + interval 60 day)
		and ws1.ws_ship_date_sk = d_date_sk
		and ws1.ws_ship_addr_sk = ca_address_sk
		and ca_state = 'OK'
		and ws1.ws_web_site_sk = web_site_sk
		and web_company_name = 'pri'
		and exists (select *
		from web_sales ws2
		where ws1.ws_order_number = ws2.ws_order_number
		and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
		and not exists(select *
		from web_returns wr1
		where ws1.ws_order_number = wr1.wr_order_number)
		order by count(distinct ws_order_number)
		limit 100;`,
	},
	{
		Query: `
-- start query 95 in stream 0 using template query95.tpl
with ws_wh as
(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from web_sales ws1,web_sales ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
 select  
   count(distinct ws_order_number) as "order count"
  ,sum(ws_ext_ship_cost) as "total shipping cost"
  ,sum(ws_net_profit) as "total net profit"
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
where
    d_date between '2001-4-01' and 
           (cast('2001-4-01' as date) + interval 60 day)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'VA'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and ws1.ws_order_number in (select ws_order_number
                            from ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from web_returns,ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
order by count(distinct ws_order_number)
limit 100;`,
	},
	{
		Query: `
		-- start query 96 in stream 0 using template query96.tpl
		select  count(*)
		from store_sales
		,household_demographics
		,time_dim, store
		where ss_sold_time_sk = time_dim.t_time_sk
		and ss_hdemo_sk = household_demographics.hd_demo_sk
		and ss_store_sk = s_store_sk
		and time_dim.t_hour = 8
		and time_dim.t_minute >= 30
		and household_demographics.hd_dep_count = 0
		and store.s_store_name = 'ese'
		order by count(*)
		limit 100;`,
	},
	{
		Query: `
-- start query 97 in stream 0 using template query97.tpl
with ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from store_sales,date_dim
where ss_sold_date_sk = d_date_sk
  and d_month_seq between 1199 and 1199 + 11
group by ss_customer_sk
        ,ss_item_sk),
csci as(
 select cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
from catalog_sales,date_dim
where cs_sold_date_sk = d_date_sk
  and d_month_seq between 1199 and 1199 + 11
group by cs_bill_customer_sk
        ,cs_item_sk)
 select  sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
limit 100;`,
	},
	{
		Query: `
		-- start query 98 in stream 0 using template query98.tpl
		select i_item_id
		,i_item_desc
		,i_category
		,i_class
		,i_current_price
		,sum(ss_ext_sales_price) as itemrevenue
		,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
	(partition by i_class) as revenueratio
		from
		store_sales
		,item
		,date_dim
		where
		ss_item_sk = i_item_sk
		and i_category in ('Men', 'Sports', 'Jewelry')
		and ss_sold_date_sk = d_date_sk
		and d_date between cast('1999-02-05' as date)
		and (cast('1999-02-05' as date) + interval 30 day)
		group by
		i_item_id
		,i_item_desc
		,i_category
		,i_class
		,i_current_price
		order by
		i_category
		,i_class
		,i_item_id
		,i_item_desc
		,revenueratio;`,
	},
	{
		Query: `
-- start query 99 in stream 0 using template query99.tpl
select  
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and 
                 (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and 
                 (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days" 
from
   catalog_sales
  ,warehouse
  ,ship_mode
  ,call_center
  ,date_dim
where
    d_month_seq between 1194 and 1194 + 11
and cs_ship_date_sk   = d_date_sk
and cs_warehouse_sk   = w_warehouse_sk
and cs_ship_mode_sk   = sm_ship_mode_sk
and cs_call_center_sk = cc_call_center_sk
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
order by substr(w_warehouse_name,1,20)
        ,sm_type
        ,cc_name
limit 100;`,
	},
}
