create or replace PACKAGE          "PKG_SALESFORCE_CATERING_SALES" 
AS
/******************************************************************************
   NAME:       PKG_SALESFORCE_CATERING_SALES
   PURPOSE:    Returns Catering sales data for salesforce integration

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        08/31/2012  Michael Zhou      Created this package.
                                           include merged accounts.
   1.1        08/15/2013  Josh Woodworth   Changed to pull pos_catering_cust_nbr as cust_acct to include placeholders   
   1.2        08/26/2013  Josh Woodworth   Changed to use the dim_ord_dest id
   1.3        09/12/2013  Josh Woodworth   Added order source desc to query results
   1.4        09/17/2013  Josh Woodworth   Added new tax,tips,null dollar amount fields plus pos_ord number field
   2.0        09/30/2013  Billy Meyers      Added new proc update_client_on_sale
   2.1        12/12/2013  Billy Meyers      Changed transaction_dt to format YYYY-MM-DD
                                            Added logic to subtract 1 hour from p_last_update_dt 
   2.2        01/09/2014  Billy Meyers     Added cafe sync procedures.
   2.3        02/18/2014  Billy Meyers     Adds cafe email to query
   2.4        03/10/2014  Billy Meyers     Check if new client exists in update sale on client
   2.5        06/16/2014  Billy Meyers     Changes pos_net_amt field from fco.pos_net_total to fco.net_tot_usd_amt
   3.0        07/16/2014  Billy Meyers     Adds discount codes to sales
   3.1        07/16/2014  Madison Cannon   Moves discount codes to seperate procedure
   3.2        07/29/2014  Billy Meyers     Optimizes discount code procedure
   3.3        09/10/2014  Madison Cannon   Adds district manager email to query
   3.4        02/02/2015  Billy Meyers     Adds proc to get sale by dw id
   3.5        03/05/2015  Billy Meyers     Adds proc to get dcr sales to process
   3.6        03/15/2015  Madison Cannon   Adds proc to count sales inserted by date 
   3.7        10/02/2015  Madison Cannon   Adds net loc amt to sales inserted by date 
   3.8        11/04/2015  Madison Cannon   Change how cafe query retirieves cafe coordinators
   3.9        04/28/2015  Madison Cannon   Change catering cafe queries to use left joins
   4.0        09/22/2016  Mark Meyer       Adds update ccd account number on Catering 3.0 sales
  -----------------------------------------------------------------------------
  Subversion Keywords
  -----------------------------------------------------------------------------
  $Author: billy.meyers $
  $HeadURL: http://svn.panerabread.com/svn/repos/DatabaseSource/EDW/pkg_salesforce_catering_sales.sql $
  $Id: pkg_salesforce_catering_sales.sql 20018 2014-06-16 16:25:35Z billy.meyers $
  $Rev: 20018 $
                                           
                                           
******************************************************************************/

   TYPE t_rec_sales_by_dt
   IS RECORD
   (
      store_nbr           ATOMIC.dim_cafe.cafe_nbr%TYPE
    , total_amt           atomic.f_cafe_ord.TOT_TXN_USD_AMT%TYPE
    , pretax_sales_amt    atomic.f_cafe_ord.GRS_USD_AMT%TYPE
    , net_loc_amt         atomic.f_cafe_ord.NET_LOC_AMT%TYPE   
    , pos_net_amt         atomic.f_cafe_ord.NET_TOT_USD_AMT%TYPE
    , tax_amt             atomic.f_cafe_ord.TAX_USD_AMT%TYPE
    , delivery_amt        atomic.f_cafe_ord.SRVC_CHG_USD_AMT%TYPE
    , gratuity_amt        atomic.f_cafe_ord.TIP_USD_AMT%TYPE
    , loc_total_amt       atomic.f_cafe_ord.TOT_TXN_LOC_AMT%TYPE
    , loc_pretax_sales_amt atomic.f_cafe_ord.GRS_LOC_AMT%TYPE
    , loc_pos_net_amt     atomic.f_cafe_ord.NET_TOT_LOC_AMT%TYPE
    , loc_tax_amt         atomic.f_cafe_ord.TAX_LOC_AMT%TYPE
    , loc_delivery_amt    atomic.f_cafe_ord.SRVC_CHG_LOC_AMT%TYPE
    , loc_gratuity_amt    atomic.f_cafe_ord.TIP_LOC_AMT%TYPE
    , pick_dlvry          atomic.dim_ord_dest.ord_dest_desc%TYPE
    , other_info          atomic.f_cafe_ord.POS_ORD_COMMENTS%TYPE
    , cust_acct           atomic.dim_catering_membership.catering_cust_nbr%TYPE
    , pos_cust_acct       atomic.f_cafe_ord.pos_catering_cust_nbr%TYPE
    , pos_on_acct_nbr     atomic.f_cafe_ord.pos_on_acct_nbr%TYPE
    , transaction_dt      atomic.dim_day.DAY%TYPE
    , emp_nbr             atomic.f_cafe_ord.pos_emp_server_id%TYPE
    , transaction_num     atomic.f_cafe_ord.DAY_DW_ID%TYPE
    , src_desc            atomic.dim_ord_src_sys.ord_src_sys_desc%TYPE
    , pos_ord_num         atomic.f_cafe_ord.pos_ord_nbr%TYPE
    , loy_card_nbr        atomic.f_cafe_ord.POS_LOYALTY_CARD_NBR%TYPE
    , src_sys_ord_nbr     atomic.f_cafe_ord.src_sys_ord_nbr%TYPE
   );

   TYPE r_cur_sales_by_dt  IS REF CURSOR RETURN t_rec_sales_by_dt;
 
   PROCEDURE open_catering_sales_by_date
   (
      p_cur                         OUT  r_cur_sales_by_dt
    , p_last_update_dt              IN   VARCHAR2
    , p_date_range_start            IN   VARCHAR2
    , p_date_range_end              IN   VARCHAR2
    );
    

    PROCEDURE open_cat_3_0_sales_by_date
   (
      p_cur                         OUT  r_cur_sales_by_dt
    , p_last_update_dt              IN   VARCHAR2
    , p_date_range_start            IN   VARCHAR2
    , p_date_range_end              IN   VARCHAR2
    );
    
    PROCEDURE open_catering_sales_by_id
   (
      p_cur                         OUT  r_cur_sales_by_dt
    , p_ord_dw_id                   IN   VARCHAR2
    );
    
    PROCEDURE count_catering_sales_by_date
    (
      p_insert_dt                   IN   VARCHAR2
    , p_date_range_start            IN   VARCHAR2
    , p_date_range_end              IN   VARCHAR2
    , p_result                      OUT  NUMBER
    ); 
    
  TYPE t_rec_dcr_sales
   IS RECORD
   (
      reassignment_id     custom_apps.sales_dest_reassignment.reassignment_id@custom_apps%TYPE
    , cafe_ord_dw_id      atomic.f_cafe_ord.CAFE_ORD_DW_ID%TYPE
    , cafe_nbr            custom_apps.sales_dest_reassignment.cafe_nbr@custom_apps%TYPE
    , order_date          VARCHAR2(10)
    , pos_ord_nbr         custom_apps.sales_dest_reassignment.pos_ord_nbr@custom_apps%TYPE
    , dest_to             custom_apps.sales_dest_reassignment.dest_to@custom_apps%TYPE
    , gross_loc_amt       custom_apps.sales_dest_reassignment.gross_loc_amt@custom_apps%TYPE
    , sf_processed_flag   custom_apps.sales_dest_reassignment.sf_processed_flag@custom_apps%TYPE
    , edw_processed_flag  custom_apps.sales_dest_reassignment.edw_processed_flag@custom_apps%TYPE
   );

   TYPE r_cur_dcr_sales  IS REF CURSOR RETURN t_rec_dcr_sales;
   
   PROCEDURE open_dcr_sales_to_process
   (
       p_cur                         OUT  r_cur_dcr_sales
   );
  
   PROCEDURE update_client_on_sale
   (
      p_business_dt                 IN VARCHAR2 
    , p_store_nbr                   IN NUMBER
	  , p_trans_no                    IN NUMBER
    , p_catering_acct               IN NUMBER
    , p_result                      OUT NUMBER
   );
   
   PROCEDURE update_client_on_cat30sale
   (
      p_business_dt                 IN VARCHAR2 
    , p_store_nbr                   IN NUMBER
	  , p_ord_dw_id                   IN NUMBER
    , p_catering_acct               IN NUMBER
    , p_result                      OUT NUMBER
   );
   
   TYPE t_rec_cafe
   IS RECORD
   (
        cafe_dw_id            ATOMIC.dim_cafe.cafe_dw_id%TYPE
      , cafe_nbr              ATOMIC.dim_cafe.cafe_nbr%TYPE
      , cafe_name             ATOMIC.dim_cafe.cafe_name%TYPE
      , cafe_email            ATOMIC.dim_cafe.cafe_email%TYPE
      , opened_date           ATOMIC.dim_cafe.opened_date%TYPE
      , closed_date           ATOMIC.dim_cafe.closed_date%TYPE
      , cafe_status           ATOMIC.dim_cafe.cafe_status%TYPE 
      , company               ATOMIC.dim_cafe.corporate_flag%TYPE
      , franchise             ATOMIC.dim_cafe.franchise%TYPE    
      , address               ATOMIC.dim_cafe.address%TYPE
      , city                  ATOMIC.dim_cafe.city%TYPE
      , state_code            ATOMIC.dim_cafe.state_code%TYPE
      , postal_code           ATOMIC.dim_cafe.postal_code%TYPE
      , country_name          ATOMIC.dim_country.country_name%TYPE      
      , phone_nbr             ATOMIC.dim_cafe.phone_nbr%TYPE         
      , region_name           ATOMIC.dim_cafe.region_name%TYPE      
      , market_name           ATOMIC.dim_cafe.market_name%TYPE    
      , district_mgrname      ATOMIC.dim_cafe.district_mgrname%TYPE
      , cafe_manager          ATOMIC.dim_cafe.cafe_manager%TYPE
      , catering_coordinators VARCHAR2(1000)
      , price_tier_code       ATOMIC.dim_cafe.price_tier_code%TYPE
      , district_mgremail     ATOMIC.dim_cafe.district_mgremail%TYPE
   );

   TYPE r_cur_cafe  IS REF CURSOR RETURN t_rec_cafe;
 
   PROCEDURE get_cafe_data
   (
      p_cur                         OUT  r_cur_cafe
   );
   
   PROCEDURE get_dead_cafes
   (
      p_cur                         OUT  r_cur_cafe
   );
   
   TYPE t_rec_discounts_by_dt
   IS RECORD
   (
      discount_code           ATOMIC.dim_std_line_item.line_item_code%TYPE
    , sale_dw_id              atomic.f_cafe_ord_line_item.CAFE_ORD_DW_ID%TYPE
    , discount_amt            atomic.f_cafe_ord_line_item.DISC_LOC_AMT%TYPE
   );
   
   TYPE r_cur_discounts_by_dt  IS REF CURSOR RETURN t_rec_discounts_by_dt;
          
   PROCEDURE get_discount_data_by_date
   (
      p_cur                         OUT  r_cur_discounts_by_dt
    , p_last_update_dt_start        IN   VARCHAR2
    , p_last_update_dt_end          IN   VARCHAR2
    , p_date_range_start            IN   VARCHAR2
    , p_date_range_end              IN   VARCHAR2
    , p_discount_codes              IN   T_TBL_DISCOUNT_CODES
   );       
END PKG_SALESFORCE_CATERING_SALES;

/
--------------------------------------------------------
--  DDL for Package Body PKG_SALESFORCE_CATERING_SALES
--------------------------------------------------------

create or replace PACKAGE BODY          "PKG_SALESFORCE_CATERING_SALES" AS
/******************************************************************************
   NAME:       PKG_SALESFORCE_CATERING_SALES
   PURPOSE:    Returns Catering sales data for salesforce integration

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        08/31/2012  Michael Zhou      Created this package.
                                           include merged accounts.
   1.1        09/22/2016  Mark Meyer        Add catering 3.0 retrieve and update member numbers
******************************************************************************/


   PROCEDURE open_catering_sales_by_date
   (
      p_cur                         OUT  r_cur_sales_by_dt
    , p_last_update_dt              IN   VARCHAR2
    , p_date_range_start            IN   VARCHAR2
    , p_date_range_end              IN   VARCHAR2
   ) AS

  BEGIN
        OPEN p_cur FOR
                SELECT  /*+ LEADING (dd) */

                                  dc.cafe_nbr                       as store_nbr
                                , fco.tot_txn_usd_amt               as total_amt
                                , fco.GRS_USD_AMT                   as pretax_sales_amt
                                , fco.net_loc_amt                   as net_loc_amt
                                , fco.NET_TOT_USD_AMT               as pos_net_amt
                                , fco.tax_usd_amt                   as tax_amt
                                , fco.srvc_chg_usd_amt              as delivery_amt
                                , fco.tip_usd_amt                   as gratuity_amt
                                , fco.tot_txn_loc_amt               as loc_total_amt
                                , fco.grs_loc_amt                   as loc_pretax_sales_amt
                                , fco.net_tot_loc_amt               as loc_pos_net_amt
                                , fco.tax_loc_amt                   as loc_tax_amt
                                , fco.srvc_chg_loc_amt              as loc_delivery_amt
                                , fco.tip_loc_amt                   as loc_gratuity_amt
                                , dod.ord_dest_desc                 as pick_dlvry
                                , fco.POS_ORD_COMMENTS              as other_info
                                , dcm.catering_cust_nbr             as cust_acct
                                , fco.pos_on_acct_nbr               as pos_on_acct_nbr
                                , fco.pos_catering_cust_nbr         as pos_cust_acct
                                , to_char(to_char(fco.pos_orig_close_dttm, 'YYYY-MM-DD'))
            --                  , fco.pos_orig_close_dttm
            --                    to_char(fco.pos_orig_close_dttm, 'Dy Mon DD HH24:MI:SS ') ||
            --                    CASE
            --                      WHEN dc.timezone ='Pacific' THEN 'PDT '
            --                      WHEN dc.timezone ='Mountain' THEN 'MDT '
            --                      WHEN dc.timezone ='Central' THEN 'CDT '
            --                      WHEN dc.timezone ='Eastern' THEN 'EDT '
            --                    ELSE 'CDT '
            --                    END
            --                    ||
            --                    to_char(fco.pos_orig_close_dttm, 'YYYY')
                                as transaction_dt

                                , fco.pos_emp_server_id               as emp_nbr
                                , fco.CAFE_ORD_DW_ID                  as transaction_num
                                , oss.ord_src_sys_desc                as src_desc
                                , fco.pos_ord_nbr                     as pos_ord_num
                                , fco.POS_LOYALTY_CARD_NBR            as loy_card_nbr
                                , fco.src_sys_ord_nbr                 as src_sys_ord_nbr
                    FROM
                                  atomic.f_cafe_ord fco
                                , atomic.dim_ord_dest dod
                                , atomic.dim_day dd
                                , atomic.dim_ord_src_sys oss
                                , atomic.dim_catering_membership dcm
                                , atomic.dim_std_ord_status sos
                                , atomic.dim_cafe dc

                        where
                                  dd.day_dw_id = fco.day_dw_id
                              and fco.UPDATE_DTTM >= to_date(p_last_update_dt, 'yyyy/mm/dd:hh:mi:ssam') - interval '1' hour
                              and dd.day >= to_date(p_date_range_start, 'yyyy/mm/dd')
                              and dd.day <= to_date(p_date_range_end, 'yyyy/mm/dd')
                              and fco.day_dw_id = dd.day_dw_id
                              and dod.ord_dest_dw_id = fco.ord_dest_dw_id
                              and fco.ord_src_sys_dw_id = oss.ord_src_sys_dw_id
                              and dod.ord_dest_code in (8,64)
                              and dcm.catering_member_dw_id = fco.catering_member_dw_id
                              and sos.STD_ORD_STATUS_DW_ID = fco.STD_ORD_STATUS_DW_ID
                              and sos.STATUS_NAME = 'Valid'
                              and dc.cafe_dw_id = fco.cafe_dw_id
            --                  and fco.pos_catering_cust_nbr > 0
                              and fco.bus_segment_dw_id = 3;

   END open_catering_sales_by_date;

  PROCEDURE open_cat_3_0_sales_by_date
   (
      p_cur                         OUT  r_cur_sales_by_dt
    , p_last_update_dt              IN   VARCHAR2
    , p_date_range_start            IN   VARCHAR2
    , p_date_range_end              IN   VARCHAR2
   ) AS

  BEGIN
        OPEN p_cur FOR
                SELECT  /*+ LEADING (dd) */

                                  dc.cafe_nbr                       as store_nbr
                                , fco.tot_txn_usd_amt               as total_amt
                                , fco.GRS_USD_AMT                   as pretax_sales_amt
                                , fco.net_loc_amt                   as net_loc_amt
                                , fco.NET_TOT_USD_AMT               as pos_net_amt
                                , fco.tax_usd_amt                   as tax_amt
                                , fco.srvc_chg_usd_amt              as delivery_amt
                                , fco.tip_usd_amt                   as gratuity_amt
                                , fco.tot_txn_loc_amt               as loc_total_amt
                                , fco.grs_loc_amt                   as loc_pretax_sales_amt
                                , fco.net_tot_loc_amt               as loc_pos_net_amt
                                , fco.tax_loc_amt                   as loc_tax_amt
                                , fco.srvc_chg_loc_amt              as loc_delivery_amt
                                , fco.tip_loc_amt                   as loc_gratuity_amt
                                , dod.ord_dest_desc                 as pick_dlvry
                                , fco.POS_ORD_COMMENTS              as other_info
                                , dcm1.catering_cust_nbr             as cust_acct
                                , fco.pos_on_acct_nbr               as pos_on_acct_nbr
                                , fco.pos_catering_cust_nbr         as pos_cust_acct
                                , to_char(to_char(fco.pos_orig_close_dttm, 'YYYY-MM-DD'))
            --                  , fco.pos_orig_close_dttm
            --                    to_char(fco.pos_orig_close_dttm, 'Dy Mon DD HH24:MI:SS ') ||
            --                    CASE
            --                      WHEN dc.timezone ='Pacific' THEN 'PDT '
            --                      WHEN dc.timezone ='Mountain' THEN 'MDT '
            --                      WHEN dc.timezone ='Central' THEN 'CDT '
            --                      WHEN dc.timezone ='Eastern' THEN 'EDT '
            --                    ELSE 'CDT '
            --                    END
            --                    ||
            --                    to_char(fco.pos_orig_close_dttm, 'YYYY')
                                as transaction_dt

                                , fco.pos_emp_server_id               as emp_nbr
                                , fco.CAFE_ORD_DW_ID                  as transaction_num
                                , oss.ord_src_sys_desc                as src_desc
                                , fco.pos_ord_nbr                     as pos_ord_num
                                , fco.POS_LOYALTY_CARD_NBR            as loy_card_nbr
                                , fco.src_sys_ord_nbr                 as src_sys_ord_nbr
                    FROM
                                  atomic.f_cafe_ord fco
                                , atomic.dim_ord_dest dod
                                , atomic.dim_day dd
                                , atomic.dim_ord_src_sys oss
                                , atomic.dim_catering_membership dcm
                                , atomic.dim_std_ord_status sos
                                , atomic.dim_cafe dc
                                , ATOMIC.DIM_CATERING_MEMBERSHIP dcm1
                        where
                                  dd.day_dw_id = fco.day_dw_id
                              and fco.UPDATE_DTTM >= to_date(p_last_update_dt, 'yyyy/mm/dd:hh:mi:ssam') - interval '1' hour
                              and dd.day >= to_date(p_date_range_start, 'yyyy/mm/dd')
                              and dd.day <= to_date(p_date_range_end, 'yyyy/mm/dd')
                              and fco.day_dw_id = dd.day_dw_id
                              and dod.ord_dest_dw_id = fco.ord_dest_dw_id
                              and fco.ord_src_sys_dw_id = oss.ord_src_sys_dw_id
                              and dod.ord_dest_code in (8,64)
                              and dcm.catering_member_dw_id = fco.catering_member_dw_id
                              and fco.POS_CUST_ACCT_ID = dcm1.HUB_CUST_ID
                              and sos.STD_ORD_STATUS_DW_ID = fco.STD_ORD_STATUS_DW_ID
                              and sos.STATUS_NAME = 'Valid'
                              and dc.cafe_dw_id = fco.cafe_dw_id
                              and fco.bus_segment_dw_id = 3
                              and oss.ORD_SRC_SYS_DESC  in ('CATERING_3_0')
                              and dcm.catering_cust_nbr = -1;

   END open_cat_3_0_sales_by_date;

   PROCEDURE open_catering_sales_by_id
   (
      p_cur                         OUT  r_cur_sales_by_dt
    , p_ord_dw_id                   IN   VARCHAR2
    )AS

  BEGIN
        OPEN p_cur FOR
                SELECT  /*+ LEADING (dd) */
                                  dc.cafe_nbr                       as store_nbr
                                , fco.tot_txn_usd_amt               as total_amt
                                , fco.GRS_USD_AMT                   as pretax_sales_amt
                                , fco.net_loc_amt                   as net_loc_amt
                                , fco.NET_TOT_USD_AMT               as pos_net_amt
                                , fco.tax_usd_amt                   as tax_amt
                                , fco.srvc_chg_usd_amt              as delivery_amt
                                , fco.tip_usd_amt                   as gratuity_amt
                                , fco.tot_txn_loc_amt               as loc_total_amt
                                , fco.grs_loc_amt                   as loc_pretax_sales_amt
                                , fco.net_tot_loc_amt               as loc_pos_net_amt
                                , fco.tax_loc_amt                   as loc_tax_amt
                                , fco.srvc_chg_loc_amt              as loc_delivery_amt
                                , fco.tip_loc_amt                   as loc_gratuity_amt
                                , dod.ord_dest_desc                 as pick_dlvry
                                , fco.POS_ORD_COMMENTS              as other_info
                                , dcm.catering_cust_nbr             as cust_acct
                                , fco.pos_catering_cust_nbr         as pos_cust_acct
                                , fco.pos_on_acct_nbr               as pos_on_acct_nbr
                                , to_char(to_char(fco.pos_orig_close_dttm, 'YYYY-MM-DD'))
            --                  , fco.pos_orig_close_dttm
            --                    to_char(fco.pos_orig_close_dttm, 'Dy Mon DD HH24:MI:SS ') ||
            --                    CASE
            --                      WHEN dc.timezone ='Pacific' THEN 'PDT '
            --                      WHEN dc.timezone ='Mountain' THEN 'MDT '
            --                      WHEN dc.timezone ='Central' THEN 'CDT '
            --                      WHEN dc.timezone ='Eastern' THEN 'EDT '
            --                    ELSE 'CDT '
            --                    END
            --                    ||
            --                    to_char(fco.pos_orig_close_dttm, 'YYYY')
                                as transaction_dt

                                , fco.pos_emp_server_id               as emp_nbr
                                , fco.CAFE_ORD_DW_ID                  as transaction_num
                                , oss.ord_src_sys_desc                as src_desc
                                , fco.pos_ord_nbr                     as pos_ord_num
                                , fco.POS_LOYALTY_CARD_NBR            as loy_card_nbr
                                , fco.src_sys_ord_nbr                 as src_sys_ord_nbr
                    FROM
                                  atomic.f_cafe_ord fco
                                , atomic.dim_ord_dest dod
                                , atomic.dim_day dd
                                , atomic.dim_ord_src_sys oss
                                , atomic.dim_catering_membership dcm
                                , atomic.dim_std_ord_status sos
                                , atomic.dim_cafe dc

                        where
                                  dd.day_dw_id = fco.day_dw_id
                              and fco.day_dw_id = dd.day_dw_id
                              and dod.ord_dest_dw_id = fco.ord_dest_dw_id
                              and fco.ord_src_sys_dw_id = oss.ord_src_sys_dw_id
                              and dod.ord_dest_code in (8,64)
                              and dcm.catering_member_dw_id = fco.catering_member_dw_id
                              and sos.STD_ORD_STATUS_DW_ID = fco.STD_ORD_STATUS_DW_ID
                              and sos.STATUS_NAME = 'Valid'
                              and dc.cafe_dw_id = fco.cafe_dw_id
            --                  and fco.pos_catering_cust_nbr > 0
                              and fco.bus_segment_dw_id = 3
                              and fco.cafe_ord_dw_id = p_ord_dw_id
                              ;

   END open_catering_sales_by_id;

   PROCEDURE count_catering_sales_by_date
    (
      p_insert_dt                   IN   VARCHAR2
    , p_date_range_start            IN   VARCHAR2
    , p_date_range_end              IN   VARCHAR2
    , p_result                      OUT  NUMBER
    )AS

  BEGIN
              SELECT COUNT(*) /*+ LEADING (dd) */
                  INTO p_result
                  FROM
                                atomic.f_cafe_ord fco
                              , atomic.dim_ord_dest dod
                              , atomic.dim_day dd
                              , atomic.dim_ord_src_sys oss
                              , atomic.dim_catering_membership dcm
                              , atomic.dim_std_ord_status sos
                              , atomic.dim_cafe dc

                      where
                                dd.day_dw_id = fco.day_dw_id
                            and fco.INSERT_DTTM >= to_date(p_insert_dt, 'yyyy/mm/dd:hh:mi:ssam')
                            and dd.day >= to_date(p_date_range_start, 'yyyy/mm/dd')
                            and dd.day <= to_date(p_date_range_end, 'yyyy/mm/dd')
                            and fco.day_dw_id = dd.day_dw_id
                            and dod.ord_dest_dw_id = fco.ord_dest_dw_id
                            and fco.ord_src_sys_dw_id = oss.ord_src_sys_dw_id
                            and dod.ord_dest_code in (8,64)
                            and dcm.catering_member_dw_id = fco.catering_member_dw_id
                            and sos.STD_ORD_STATUS_DW_ID = fco.STD_ORD_STATUS_DW_ID
                            and sos.STATUS_NAME = 'Valid'
                            and dc.cafe_dw_id = fco.cafe_dw_id
          --                  and fco.pos_catering_cust_nbr > 0
                            and fco.bus_segment_dw_id = 3;

   END count_catering_sales_by_date;

   PROCEDURE open_dcr_sales_to_process
   (
       p_cur                         OUT  r_cur_dcr_sales
   ) AS

   BEGIN
      OPEN p_cur FOR
          SELECT
              sdr.reassignment_id         reassignment_id,
              fco.cafe_ord_dw_id          cafe_ord_dw_id,
              sdr.cafe_nbr                cafe_nbr,
              to_char(sdr.order_date,
                          'YYYY-MM-DD')   order_date,
              sdr.pos_ord_nbr             pos_ord_nbr,
              sdr.dest_to                 dest_to,
              sdr.gross_loc_amt           gross_loc_amt,
              sdr.sf_processed_flag       sf_processed_flag,
              sdr.edw_processed_flag      edw_processed_flag
          FROM
                custom_apps.sales_dest_reassignment@custom_apps sdr
              , atomic.f_cafe_ord fco
              , atomic.dim_cafe dc
              , atomic.dim_day dd
          WHERE
                  fco.day_dw_id = dd.day_dw_id
              AND fco.cafe_dw_id = dc.cafe_dw_id
              AND dd.day = sdr.order_date
              AND dc.cafe_nbr = sdr.cafe_nbr
              AND fco.pos_ord_nbr = sdr.pos_ord_nbr
              AND sdr.sf_processed_flag = 0
              AND sdr.edw_processed_flag = 1

            ORDER BY sdr.reassignment_id ASC;

   END open_dcr_sales_to_process;

   PROCEDURE update_client_on_sale
   (
      p_business_dt                 IN VARCHAR2
    , p_store_nbr                   IN NUMBER
    , p_trans_no                    IN NUMBER
    , p_catering_acct               IN NUMBER
    , p_result                      OUT NUMBER
   ) AS

     v_error                  CONSTANT NUMBER:= -1;
     v_error_client_not_found CONSTANT NUMBER:= -2;

     v_day_dw_id              atomic.dim_star_day.day_dw_id%TYPE;
     v_cafe_dw_id             atomic.dim_cafe.cafe_dw_id%TYPE;
     v_catering_member_dw_id  atomic.dim_catering_membership.catering_member_dw_id%TYPE;
     v_client_exists          NUMBER;

     c_proc_name   CONSTANT VARCHAR2 (30) := 'update_client_on_sale';
     o_aud                  obj_aud       := obj_aud (0, 0, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, NULL, NULL);
     pl_sql_id              NUMBER;

   BEGIN
      pl_sql_id := 0;
      obj_aud.insert_rec (o_aud, 1800);
      pl_sql_id := 10;

      /*Confirm that new client is in dim_catering */
      SELECT count(*)
      INTO   v_client_exists
      FROM   atomic.dim_catering_membership dcm
      WHERE  dcm.catering_cust_nbr = p_catering_acct;

      IF v_client_exists > 0 THEN
        /*Retrieve day_dw_id, cafe_dw_id, and catering_member_dw_id*/
        SELECT dd.day_dw_id,
               dc.cafe_dw_id,
               dcm.catering_member_dw_id
          INTO   v_day_dw_id,
                 v_cafe_dw_id,
                 v_catering_member_dw_id
          FROM   atomic.dim_star_day dd
               , atomic.dim_cafe dc
               , atomic.dim_catering_membership dcm
          WHERE  dc.cafe_nbr = p_store_nbr
          and    dd.day = to_date(p_business_dt, 'YYYY-MM-DD')
          and    dcm.catering_cust_nbr = p_catering_acct;

        pl_sql_id := 20;

        /*Update client in f_cafe_ord table*/
        UPDATE  atomic.f_cafe_ord
          SET     catering_member_dw_id = v_catering_member_dw_id
                , update_dttm = SYSDATE
                , VERSION = VERSION + 1
          WHERE  day_dw_id = v_day_dw_id
          and    cafe_dw_id = v_cafe_dw_id
          and    pos_ord_nbr = p_trans_no;

        /*Procedure returns number of rows updated in f_cafe_ord*/
        p_result := SQL%ROWCOUNT;
        pl_sql_id := 30;

        /*Update client in f_cafe_ord_line_item table*/
        UPDATE  ATOMIC.f_cafe_ord_line_item
          SET     catering_member_dw_id = v_catering_member_dw_id
                , update_dttm = SYSDATE
                , VERSION = VERSION + 1
          WHERE  day_dw_id = v_day_dw_id
          and    cafe_dw_id = v_cafe_dw_id
          and    pos_ord_nbr = p_trans_no;

        pl_sql_id := 40;
        COMMIT;
      ELSE
        p_result := v_error_client_not_found;
      END IF;

      pl_sql_id := 50;
      obj_aud.update_rec (o_aud);
      EXCEPTION
        WHEN OTHERS THEN
            o_aud.plsql_error_desc := SQLERRM;
            o_aud.plsql_error_loc := pl_sql_id;
            obj_aud.update_rec (o_aud);
            p_result := v_error;

   END update_client_on_sale;

   PROCEDURE update_client_on_cat30sale
   (
      p_business_dt                 IN VARCHAR2
    , p_store_nbr                   IN NUMBER
    , p_ord_dw_id                   IN NUMBER
    , p_catering_acct               IN NUMBER
    , p_result                      OUT NUMBER
   ) AS

     v_error                  CONSTANT NUMBER:= -1;
     v_error_client_not_found CONSTANT NUMBER:= -2;

     v_day_dw_id              atomic.dim_star_day.day_dw_id%TYPE;
     v_cafe_dw_id             atomic.dim_cafe.cafe_dw_id%TYPE;
     v_catering_member_dw_id  atomic.dim_catering_membership.catering_member_dw_id%TYPE;
     v_client_exists          NUMBER;

     c_proc_name   CONSTANT VARCHAR2 (30) := 'update_client_on_cat30sale';
     o_aud                  obj_aud       := obj_aud (0, 0, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, NULL, NULL);
     pl_sql_id              NUMBER;

   BEGIN
      pl_sql_id := 0;
      obj_aud.insert_rec (o_aud, 1801);
      pl_sql_id := 10;


      /*Confirm that new client is in dim_catering */
      SELECT count(*)
      INTO   v_client_exists
      FROM   atomic.dim_catering_membership dcm
      WHERE  dcm.catering_cust_nbr = p_catering_acct;

      IF v_client_exists > 0 THEN

        /*Retrieve day_dw_id, cafe_dw_id, and catering_member_dw_id*/
        SELECT dd.day_dw_id,
               dc.cafe_dw_id,
               dcm.catering_member_dw_id
          INTO   v_day_dw_id,
                 v_cafe_dw_id,
                 v_catering_member_dw_id
          FROM   atomic.dim_star_day dd
               , atomic.dim_cafe dc
               , atomic.dim_catering_membership dcm
          WHERE  dc.cafe_nbr = p_store_nbr
          and    dd.day = to_date(p_business_dt, 'YYYY-MM-DD')
          and    dcm.catering_cust_nbr = p_catering_acct;

        pl_sql_id := 20;

        /*Update client in f_cafe_ord table*/
        UPDATE  atomic.f_cafe_ord
          SET     catering_member_dw_id = v_catering_member_dw_id
                , update_dttm = SYSDATE
                , VERSION = VERSION + 1
          WHERE  day_dw_id = v_day_dw_id
          and    cafe_dw_id = v_cafe_dw_id
          and    cafe_ord_dw_id = p_ord_dw_id;

        /*Procedure returns number of rows updated in f_cafe_ord*/
        p_result := SQL%ROWCOUNT;
        pl_sql_id := 30;

        /*Update client in f_cafe_ord_line_item table*/
        UPDATE  ATOMIC.f_cafe_ord_line_item
          SET     catering_member_dw_id = v_catering_member_dw_id
                , update_dttm = SYSDATE
                , VERSION = VERSION + 1
          WHERE  day_dw_id = v_day_dw_id
          and    cafe_dw_id = v_cafe_dw_id
          and    cafe_ord_dw_id = p_ord_dw_id;

        pl_sql_id := 40;
        COMMIT;
      ELSE
        p_result := v_error_client_not_found;
      END IF;

      pl_sql_id := 50;
      obj_aud.update_rec (o_aud);
      EXCEPTION
        WHEN OTHERS THEN
            o_aud.plsql_error_desc := SQLERRM;
            o_aud.plsql_error_loc := pl_sql_id;
            obj_aud.update_rec (o_aud);
            p_result := v_error;

   END update_client_on_cat30sale;

    PROCEDURE get_cafe_data
   (
      p_cur                         OUT  r_cur_cafe
   ) AS

    BEGIN
          OPEN p_cur FOR
              SELECT
                      dca.cafe_dw_id              as cafe_dw_id
                    , dca.cafe_nbr                as cafe_nbr
                    , dca.cafe_name               as cafe_name
                    , dca.cafe_email              as cafe_email
                    , to_char(dca.opened_date, 'YYYY-MM-DD')  as opened_date
                    , to_char(dca.closed_date, 'YYYY-MM-DD')  as closed_date
                    , dca.cafe_status             as cafe_status
                    , dca.corporate_flag          as company
                    , dca.franchise               as franchise
                    , dca.address                 as address
                    , dca.city                    as city
                    , dca.state_code              as state_code
                    , dca.postal_code             as postal_code
                    , dco.country_name            as country_name
                    , dca.phone_nbr               as phone_nbr
                    , dca.region_name             as region_name
                    , dca.market_name             as market_name
                    , dca.district_mgrname        as district_mgrname
                    , dca.cafe_manager            as cafe_manager
                    , cc.catering_coordinators    as catering_coordinators
                    , dca.price_tier_code         as price_tier_code
                    , dca.district_mgremail       as district_mgremail

              from    ATOMIC.dim_cafe dca
                      left join ATOMIC.dim_country dco
                      on dca.country_dw_id = dco.country_dw_id
                      left join (SELECT
                        dep.dept_code AS cafe_nbr,
                        LISTAGG(de.first_name || ' ' || de.last_name || ' ' || de.employee_nbr, '|') WITHIN GROUP (ORDER BY de.first_name || ' ' || de.last_name || ' ' || de.employee_nbr ) AS catering_coordinators
                       FROM atomic.dim_employee_wd de
                        , (SELECT das.employee_dw_id, djc.job_code, djc.job_desc
                           FROM atomic.dim_assignment_span_wd das
                            , atomic.dim_assignment_span_type_wd dast
                            , atomic.dim_job_code_wd djc
                          WHERE das.span_type_dw_id = dast.span_type_dw_id
                            AND dast.span_type_code = 'JOB_CODE_DW_ID'
                            AND das.span_value = djc.job_code_dw_id
                            AND djc.job_desc = 'Catering Coordinator'
                            AND SYSDATE BETWEEN das.effective_start_date AND das.effective_end_date) jc
                        , (SELECT das.employee_dw_id, SUBSTR (dd.dept_code, 1, 6) AS dept_code
                          FROM atomic.dim_assignment_span_wd das
                           , atomic.dim_assignment_span_type_wd dast
                           , atomic.dim_dept_wd dd
                          WHERE das.span_type_dw_id = dast.span_type_dw_id
                            AND dast.span_type_code = 'DEPT_DW_ID'
                            AND das.span_value = dd.dept_dw_id
                            AND SYSDATE BETWEEN das.effective_start_date AND das.effective_end_date
                            AND REGEXP_LIKE (SUBSTR (dd.dept_code, 1, 6), '^[[:digit:]]{6}$')) dep
                       WHERE de.employee_dw_id = jc.employee_dw_id AND de.employee_dw_id = dep.employee_dw_id
                       GROUP BY dep.dept_code
                       ORDER BY dep.dept_code) cc
                      on to_char(dca.cafe_nbr) = cc.cafe_nbr
              where dca.cafe_nbr > 0
              and dca.cafe_status not like '%Dead%'
              ;

    END get_cafe_data;

    PROCEDURE get_dead_cafes
   (
      p_cur                         OUT  r_cur_cafe
   ) AS

    BEGIN
          OPEN p_cur FOR
              SELECT
                      dca.cafe_dw_id              as cafe_dw_id
                    , dca.cafe_nbr                as cafe_nbr
                    , dca.cafe_name               as cafe_name
                    , dca.cafe_email              as cafe_email
                    , to_char(dca.opened_date, 'YYYY-MM-DD')  as opened_date
                    , to_char(dca.closed_date, 'YYYY-MM-DD')  as closed_date
                    , dca.cafe_status             as cafe_status
                    , dca.corporate_flag          as company
                    , dca.franchise               as franchise
                    , dca.address                 as address
                    , dca.city                    as city
                    , dca.state_code              as state_code
                    , dca.postal_code             as postal_code
                    , dco.country_name            as country_name
                    , dca.phone_nbr               as phone_nbr
                    , dca.region_name             as region_name
                    , dca.market_name             as market_name
                    , dca.district_mgrname        as district_mgrname
                    , dca.cafe_manager            as cafe_manager
                    , cc.catering_coordinators    as catering_coordinators
                    , dca.price_tier_code         as price_tier_code
                    , dca.district_mgremail       as district_mgremail

              from    ATOMIC.dim_cafe dca
                      left join ATOMIC.dim_country dco
                      on dca.country_dw_id = dco.country_dw_id
                      left join (SELECT
                        dep.dept_code AS cafe_nbr,
                        LISTAGG(de.first_name || ' ' || de.last_name || ' ' || de.employee_nbr, '|') WITHIN GROUP (ORDER BY de.first_name || ' ' || de.last_name || ' ' || de.employee_nbr ) AS catering_coordinators
                       FROM atomic.dim_employee_wd de
                        , (SELECT das.employee_dw_id, djc.job_code, djc.job_desc
                           FROM atomic.dim_assignment_span_wd das
                            , atomic.dim_assignment_span_type_wd dast
                            , atomic.dim_job_code_wd djc
                          WHERE das.span_type_dw_id = dast.span_type_dw_id
                            AND dast.span_type_code = 'JOB_CODE_DW_ID'
                            AND das.span_value = djc.job_code_dw_id
                            AND djc.job_desc = 'Catering Coordinator'
                            AND SYSDATE BETWEEN das.effective_start_date AND das.effective_end_date) jc
                        , (SELECT das.employee_dw_id, SUBSTR (dd.dept_code, 1, 6) AS dept_code
                          FROM atomic.dim_assignment_span_wd das
                           , atomic.dim_assignment_span_type_wd dast
                           , atomic.dim_dept_wd dd
                          WHERE das.span_type_dw_id = dast.span_type_dw_id
                            AND dast.span_type_code = 'DEPT_DW_ID'
                            AND das.span_value = dd.dept_dw_id
                            AND SYSDATE BETWEEN das.effective_start_date AND das.effective_end_date
                            AND REGEXP_LIKE (SUBSTR (dd.dept_code, 1, 6), '^[[:digit:]]{6}$')) dep
                       WHERE de.employee_dw_id = jc.employee_dw_id AND de.employee_dw_id = dep.employee_dw_id
                       GROUP BY dep.dept_code
                       ORDER BY dep.dept_code) cc
                       on to_char(dca.cafe_nbr) = cc.cafe_nbr
              where dca.cafe_nbr > 0
              and dca.cafe_status like '%Dead%'
              and dca.update_dttm >= trunc(sysdate - 7)
              ;

    END get_dead_cafes;

    PROCEDURE get_discount_data_by_date
    (
        p_cur                         OUT  r_cur_discounts_by_dt
      , p_last_update_dt_start        IN   VARCHAR2
      , p_last_update_dt_end          IN   VARCHAR2
      , p_date_range_start            IN   VARCHAR2
      , p_date_range_end              IN   VARCHAR2
      , p_discount_codes              IN   T_TBL_DISCOUNT_CODES
    ) AS

      BEGIN
        OPEN p_cur FOR
            SELECT
                   dsli.line_item_code discount_code
                 , fcoli.CAFE_ORD_DW_ID    sale_dw_id
                 , fcoli.DISC_LOC_AMT      discount_amt
            FROM
                   atomic.f_cafe_ord_line_item fcoli
                 , atomic.dim_day dd
                 , ATOMIC.dim_std_line_item dsli
                 , ATOMIC.DIM_STD_ORD_STATUS sos
                 , atomic.dim_ord_dest dod
            WHERE fcoli.UPDATE_DTTM >= to_date(p_last_update_dt_start, 'yyyy/mm/dd:hh:mi:ssam') - interval '1' hour
            and fcoli.UPDATE_DTTM <= to_date(p_last_update_dt_end, 'yyyy/mm/dd:hh:mi:ssam')
            and dd.day >= to_date(p_date_range_start, 'yyyy/mm/dd')
            and dd.day <= to_date(p_date_range_end, 'yyyy/mm/dd')
            and dsli.line_item_type = 'DISCOUNT'
            and dsli.line_item_code in ( SELECT /*+ cardinality(t 1) */ t.COLUMN_VALUE FROM TABLE(p_discount_codes) t )
            and sos.STATUS_NAME = 'Valid'
            and dod.ord_dest_code in (8,64)
            and DD.day_dw_id = fcoli.day_dw_id
            and dsli.std_line_dw_id = fcoli.std_line_dw_id
            and sos.STD_ORD_STATUS_DW_ID = fcoli.STD_ORD_STATUS_DW_ID
            and dod.ord_dest_dw_id = fcoli.ord_dest_dw_id
            ;

    END get_discount_data_by_date;


END PKG_SALESFORCE_CATERING_SALES;