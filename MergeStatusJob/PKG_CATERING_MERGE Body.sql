--------------------------------------------------------
--  File created - Monday-September-12-2016   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package Body PKG_CATERING_MERGE
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "SOURCE"."PKG_CATERING_MERGE" 
IS
   c_pkg_name   CONSTANT VARCHAR2 (30) := 'pkg_catering_merge';

   TYPE merge_row_type IS TABLE OF stage.dim_catering_member_merge%ROWTYPE;

   FUNCTION add_merge_request (
      p_old_catering_cust_nbr   IN   NUMBER,
      p_new_catering_cust_nbr   IN   NUMBER,
      p_merge_seq               IN   VARCHAR2)
      RETURN VARCHAR2
   /*---------------------------------------------------------------------------
     This will merge the unit cost and related columns into the
     f_cafe_ord_line_item table. It will also update / reload any downstream
     values needed.
     -------------------------------------------------------------------------*/
   IS
      c_proc_name   CONSTANT VARCHAR2 (30) := 'add_merge_request';
      o_aud                  obj_aud       := obj_aud (0, 0, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, NULL, NULL);
      pl_sql_id              NUMBER;
   BEGIN
      pl_sql_id := 0;
      obj_aud.insert_rec (o_aud, 1800);
      pl_sql_id := 10;

      INSERT INTO stage.dim_catering_member_merge
                  (old_catering_cust_nbr,
                   new_catering_cust_nbr,
                   merge_seq,
                   status,
                   insert_dttm,
                   update_dttm,
                   VERSION,
                   load_dw_id)
           VALUES (p_old_catering_cust_nbr,
                   p_new_catering_cust_nbr,
                   p_merge_seq,
                   'NEW',
                   SYSDATE,
                   SYSDATE,
                   0,
                   o_aud.load_id);

      pl_sql_id := 20;
      COMMIT;
      pl_sql_id := 30;
      RETURN 'Success';
      pl_sql_id := 40;
      obj_aud.update_rec (o_aud);
   EXCEPTION
      WHEN OTHERS
      THEN
         o_aud.plsql_error_desc := SQLERRM;
         o_aud.plsql_error_loc := pl_sql_id;
         obj_aud.update_rec (o_aud);
         RETURN o_aud.plsql_error_desc;
   END add_merge_request;

   PROCEDURE process_merge_request (p_merge_row IN stage.dim_catering_member_merge%ROWTYPE)
   IS
      c_proc_name   CONSTANT VARCHAR2 (30) := 'process_merge_request';
      o_aud                  obj_aud       := obj_aud (0, 0, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, NULL, NULL);
      pl_sql_id              NUMBER;
      v_old_dw_id            NUMBER;
      v_new_dw_id            NUMBER;
   BEGIN
      pl_sql_id := 0;
      obj_aud.insert_rec (o_aud, 1801);
      pl_sql_id := 10;

      /* Get the old and new catering_member_dw_id values */
      BEGIN
      SELECT dcm.catering_member_dw_id
        INTO v_old_dw_id
        FROM atomic.dim_catering_membership dcm
       WHERE dcm.catering_cust_nbr = p_merge_row.old_catering_cust_nbr;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN v_old_dw_id := NULL;
      END;  

      pl_sql_id := 20;
      
      BEGIN
      SELECT dcm.catering_member_dw_id
        INTO v_new_dw_id
        FROM atomic.dim_catering_membership dcm
       WHERE dcm.catering_cust_nbr = p_merge_row.new_catering_cust_nbr;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN v_new_dw_id := NULL;
      END;  
      
      pl_sql_id := 30;

      IF v_old_dw_id IS NOT NULL AND v_new_dw_id IS NOT NULL
      THEN
         pl_sql_id := 40;

         /* Update the old entry in dim_catering_membership to map any new
            orders coming in from the cafe with the old catering number
            to the new catering customer number */
         UPDATE atomic.dim_catering_membership
            SET catering_member_dw_id_xref = v_new_dw_id,
                update_dttm = SYSDATE,
                VERSION = VERSION + 1
          WHERE catering_member_dw_id_xref = v_old_dw_id;

         pl_sql_id := 50;

         /* Update any orders in f_cafe_ord_line_item with the old catering_member_dw_id to
            us the new one */
         UPDATE atomic.f_cafe_ord_line_item
            SET catering_member_dw_id = v_new_dw_id,
                update_dttm = SYSDATE,
                VERSION = VERSION + 1
          WHERE cafe_ord_line_item_dw_id IN (
                   SELECT fcoli.cafe_ord_line_item_dw_id
                     FROM atomic.dim_catering_membership dcm,
                          atomic.f_cafe_ord fco,
                          atomic.f_cafe_ord_line_item fcoli
                    WHERE dcm.catering_member_dw_id_xref = v_new_dw_id
                      AND dcm.catering_member_dw_id = fco.catering_member_dw_id
                      AND fco.catering_member_dw_id != v_new_dw_id
                      AND fco.day_dw_id = fcoli.day_dw_id
                      AND fco.pos_ord_nbr = fcoli.pos_ord_nbr
                      AND fco.cafe_dw_id = fcoli.cafe_dw_id);

         pl_sql_id := 60;

         /* Update any orders in f_cafe_ord with the old catering_member_dw_id to
            us the new one */
         UPDATE atomic.f_cafe_ord
            SET catering_member_dw_id = v_new_dw_id
          WHERE cafe_ord_dw_id IN (
                   SELECT fco.cafe_ord_dw_id
                     FROM atomic.dim_catering_membership dcm,
                          atomic.f_cafe_ord fco
                    WHERE dcm.catering_member_dw_id_xref = v_new_dw_id
                      AND dcm.catering_member_dw_id = fco.catering_member_dw_id
                      AND fco.catering_member_dw_id != v_new_dw_id);

         pl_sql_id := 70;

         /* Update the status in the staging table to LOADED */
         UPDATE stage.dim_catering_member_merge
            SET status = 'LOADED',
                update_dttm = SYSDATE,
                VERSION = VERSION + 1,
                load_dw_id = o_aud.load_id
          WHERE merge_seq = p_merge_row.merge_seq;

         COMMIT;
      ELSE
         pl_sql_id := 80;

         /* There was a problem looking up one of the catering member numbers in
            atomic.dim_catering_membership.  If the staging record is less than a week
            old then set its status to FAIL1.  We will re-attempt to merge these.
            Otherwise, set it to FAIL2 and we will not re-attempt to merge these. */
         UPDATE stage.dim_catering_member_merge
            SET status =
                   CASE
                      WHEN status = 'INWORK'
                         THEN 'FAIL1'
                      WHEN status = 'FAIL1' AND insert_dttm > TRUNC (SYSDATE) - 7
                         THEN 'FAIL1'
                      ELSE 'FAIL2'
                   END,
                update_dttm = SYSDATE,
                VERSION = VERSION + 1,
                load_dw_id = o_aud.load_id
          WHERE merge_seq = p_merge_row.merge_seq;
      END IF;

      pl_sql_id := 90;
      COMMIT;
      pl_sql_id := 100;
      obj_aud.update_rec (o_aud);
   EXCEPTION
      WHEN OTHERS
      THEN
         o_aud.plsql_error_desc := SQLERRM;
         o_aud.plsql_error_loc := pl_sql_id;
         obj_aud.update_rec (o_aud);
         DBMS_OUTPUT.put_line ('ERROR in section ' || TO_CHAR (pl_sql_id) || ' in ' || c_pkg_name || '.' || c_proc_name);
         DBMS_OUTPUT.put_line (SQLERRM);
         RAISE;
   END process_merge_request;

   PROCEDURE process_merge_requests
   /*---------------------------------------------------------------------------
     This will merge the unit cost and related columns into the
     f_cafe_ord_line_item table. It will also update / reload any downstream
     values needed.
     -------------------------------------------------------------------------*/
   IS
      c_proc_name   CONSTANT VARCHAR2 (30)  := 'process_merge_requests';
      o_aud                  obj_aud        := obj_aud (0, 0, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, NULL, NULL);
      pl_sql_id              NUMBER;
      merge_rows             merge_row_type;
   BEGIN
      pl_sql_id := 0;
      obj_aud.insert_rec (o_aud, 1802);
      pl_sql_id := 10;

      /* Set NEW merge requests to INWORK and commit the change */
      /* This will allow us to update the status on the merge requests one by one */
      UPDATE stage.dim_catering_member_merge
         SET status = 'INWORK'
       WHERE status = 'NEW';

      pl_sql_id := 20;
      COMMIT;
      pl_sql_id := 30;

      /* Gather all mrege requests to be worked on in a collection. */
      SELECT   mrg.*
      BULK COLLECT INTO merge_rows
          FROM stage.dim_catering_member_merge mrg
         WHERE mrg.status IN ('INWORK', 'FAIL1')
      ORDER BY mrg.merge_seq;

      pl_sql_id := 40;

      IF SQL%ROWCOUNT > 0
      THEN
         pl_sql_id := 50;

         /* Process each merge request one by one. */
         FOR ii IN merge_rows.FIRST .. merge_rows.LAST
         LOOP
            pl_sql_id := 60;
            process_merge_request (merge_rows (ii));
         END LOOP;
      END IF;

      pl_sql_id := 70;
      obj_aud.update_rec (o_aud);
   EXCEPTION
      WHEN OTHERS
      THEN
         o_aud.plsql_error_desc := SQLERRM;
         o_aud.plsql_error_loc := pl_sql_id;
         obj_aud.update_rec (o_aud);
         DBMS_OUTPUT.put_line ('ERROR in section ' || TO_CHAR (pl_sql_id) || ' in ' || c_pkg_name || '.' || c_proc_name);
         DBMS_OUTPUT.put_line (SQLERRM);
         RAISE;
   END process_merge_requests;
   
  PROCEDURE get_updated_merge_requests (
      p_cur             OUT   r_cur_updated_merge_requests,
      p_last_update_dt  IN    VARCHAR2)
  /*---------------------------------------------------------------------------
  This will retrieve merge requests that have been updated since the date
  param that is passed in
  -------------------------------------------------------------------------*/
  IS
  BEGIN
    OPEN p_cur FOR 
      SELECT merge_seq, 
             DECODE (status, 'LOADED', 'Success'
                           , 'FAIL1', status || '-' || version
                           , status) status
        FROM STAGE.dim_catering_member_merge 
       WHERE update_dttm >= to_date(p_last_update_dt, 'yyyy/mm/dd:hh:mi:ssam') - interval '1' hour;
  END get_updated_merge_requests;
   
END pkg_catering_merge;

/
