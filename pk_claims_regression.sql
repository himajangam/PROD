CREATE OR REPLACE PACKAGE pk_claims_regression
AS
   PROCEDURE pr_batch_creation (
      p_tcn                  IN       CLOB,
      p_desc                 IN       VARCHAR2,
      p_batch_instance_sid   OUT      batch_instance.batch_instance_sid%TYPE
   );
   
   PROCEDURE pr_insertregprodmap (p_batch_id IN NUMBER);
   
   

   PROCEDURE pr_cleanupclaim (p_batch_id IN NUMBER, p_created_by IN NUMBER);

   PROCEDURE pr_poll_reg_new (
      p_created_by   IN       NUMBER,
      p_err_code     OUT      VARCHAR2,
      p_err_msg      OUT      VARCHAR2,
      p_batch_id     IN       NUMBER,
      p_reg_flag     IN       CHAR
   );

   PROCEDURE pr_update_change_flag (
   p_batch_id in number,
      p_created_by   IN       NUMBER,
      p_err_code     OUT      VARCHAR2,
      p_err_msg      OUT      VARCHAR2
   );
   
 PROCEDURE PR_REGRESSION_CLEANUP_NEW(
   p_claim_header_sid   IN       claim_header.claim_header_sid%TYPE,
   p_created_by         IN       NUMBER,
   p_errcode            OUT      VARCHAR2,
   p_errmsg             OUT      VARCHAR2
);
   
END pk_claims_regression;
/



CREATE OR REPLACE PACKAGE BODY pk_claims_regression
AS
   PROCEDURE pr_insertregprodmap (p_batch_id IN NUMBER)
   IS
      v_tcn                 clob;
      v_domain_code_array   pk_array_type.v_str_arr;
      v_err_code            VARCHAR2 (3200);
      v_err_msg             VARCHAR2 (3200);
    
    
   BEGIN
   
        BEGIN
         SELECT job_actual_param_value1
           INTO v_tcn
           FROM batch_instance_x_job_param
          WHERE batch_instance_sid = p_batch_id
            AND job_param_sid =
                   (SELECT job_param_sid
                      FROM job_parameter
                     WHERE batch_job_sid IN (
                               SELECT batch_job_sid
                                 FROM batch_job
                                WHERE batch_job_name =
                                                      'REGREESION_TEST_CLAIMS')
                       AND job_param_name = 'UPLOADED_TCNS');
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error (SQLCODE, 'no tcn in batch');
         when others then
         raise_application_error (SQLCODE, 'too many tcns');   
      END;

      pr_parse_list_clob (v_tcn, ',', v_domain_code_array, v_err_code, v_err_msg);

      IF (v_err_code <> 0)
      THEN
        raise_application_error (SQLCODE, 'error in pr_parse_list');
         RETURN;
      END IF;

      
      FOR i IN v_domain_code_array.FIRST .. v_domain_code_array.LAST
      LOOP
         INSERT INTO reg_prod_map
                     (reg_prod_map_sid, prod_claim_header_sid,
                      reg_claim_header_sid, prod_tcn, reg_tcn, reg_batch_id,
                      run_number, status_type_cid, status_cid, created_by,
                      created_date, modified_by, modified_date,
                      invoice_type_lkpcd)
            SELECT reg_prod_map_seq.NEXTVAL, claim_header_sid,
                   claim_header_sid, TRIM (v_domain_code_array (i)),
                   TRIM (v_domain_code_array (i)), p_batch_id, last_run_nmbr,
                   84, 4, 1, SYSDATE, 1, SYSDATE, invoice_type_lkpcd
              FROM ad_claim_header
             WHERE tcn = TRIM (v_domain_code_array (i));

         IF SQL%ROWCOUNT = 0
         THEN
            INSERT INTO reg_prod_map
                        (reg_prod_map_sid, prod_claim_header_sid,
                         reg_claim_header_sid, prod_tcn, reg_tcn,
                         reg_batch_id, run_number, status_type_cid,
                         status_cid, created_by, created_date, modified_by,
                         modified_date, invoice_type_lkpcd)
               SELECT reg_prod_map_seq.NEXTVAL, claim_header_sid,
                      claim_header_sid, TRIM (v_domain_code_array (i)),
                      TRIM (v_domain_code_array (i)), p_batch_id,
                      last_run_nmbr, 84, 4, 1, SYSDATE, 1, SYSDATE,
                      invoice_type_lkpcd
                 FROM claim_header
                WHERE tcn = TRIM (v_domain_code_array (i));
         END IF;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         raise_application_error (SQLCODE, SQLERRM);
   END;

   PROCEDURE pr_batch_creation (
      p_tcn                  IN       CLOB,
      p_desc                 IN       VARCHAR2,
      p_batch_instance_sid   OUT      batch_instance.batch_instance_sid%TYPE
   )
   IS
   BEGIN
      INSERT INTO batch_instance
                  (batch_instance_sid,
                   batch_job_sid, status,
                   start_date_time, end_date_time, oprtnl_flag, created_by,
                   created_date, modified_by, modified_date
                  )
           VALUES (batch_instance_seq.NEXTVAL,
                   (SELECT batch_job_sid
                      FROM batch_job
                     WHERE batch_job_name = 'REGREESION_TEST_CLAIMS'), 'M',
                   SYSDATE, SYSDATE, 'A', 1,
                   SYSDATE, 1, SYSDATE
                  )
        RETURNING batch_instance_sid
             INTO p_batch_instance_sid;

      INSERT INTO batch_instance_x_job_param
                  (batch_instance_x_job_param_sid,
                   batch_instance_sid,
                   job_param_sid,
                   serial_nmbr, job_actual_param_value, oprtnl_flag,
                   created_by, created_date, modified_by, modified_date,
                   job_actual_param_value1
                  )
           VALUES (batch_instance_x_job_param_seq.NEXTVAL,
                   p_batch_instance_sid,
                   (SELECT job_param_sid
                      FROM job_parameter
                     WHERE batch_job_sid IN (
                               SELECT batch_job_sid
                                 FROM batch_job
                                WHERE batch_job_name =
                                                      'REGREESION_TEST_CLAIMS')
                       AND job_param_name = 'UPLOADED_TCNS'),
                   1, 'Value stored in JOB_ACTUAL_PARAM_VALUE1', 'A',
                   1, SYSDATE, 1, SYSDATE,
                   p_tcn
                  );

      INSERT INTO batch_instance_x_job_param
                  (batch_instance_x_job_param_sid,
                   batch_instance_sid,
                   job_param_sid,
                   serial_nmbr, job_actual_param_value, oprtnl_flag,
                   created_by, created_date, modified_by, modified_date
                  )
           VALUES (batch_instance_x_job_param_seq.NEXTVAL,
                   p_batch_instance_sid,
                   (SELECT job_param_sid
                      FROM job_parameter
                     WHERE batch_job_sid IN (
                               SELECT batch_job_sid
                                 FROM batch_job
                                WHERE batch_job_name =
                                                      'REGREESION_TEST_CLAIMS')
                       AND job_param_name = 'SOURCE_DATABASE'),
                   1, 'Regression_' || SYSDATE, 'A',
                   1, SYSDATE, 1, SYSDATE
                  );

      INSERT INTO batch_instance_x_job_param
                  (batch_instance_x_job_param_sid,
                   batch_instance_sid,
                   job_param_sid,
                   serial_nmbr, job_actual_param_value, oprtnl_flag,
                   created_by, created_date, modified_by, modified_date
                  )
           VALUES (batch_instance_x_job_param_seq.NEXTVAL,
                   p_batch_instance_sid,
                   (SELECT job_param_sid
                      FROM job_parameter
                     WHERE batch_job_sid IN (
                               SELECT batch_job_sid
                                 FROM batch_job
                                WHERE batch_job_name =
                                                      'REGREESION_TEST_CLAIMS')
                       AND job_param_name = 'COPY_CLM_OR_CLMANDDEPENDENCIES'),
                   1, 'CO', 'A',
                   1, SYSDATE, 1, SYSDATE
                  );

      INSERT INTO batch_instance_x_job_param
                  (batch_instance_x_job_param_sid,
                   batch_instance_sid,
                   job_param_sid,
                   serial_nmbr, job_actual_param_value, oprtnl_flag,
                   created_by, created_date, modified_by, modified_date
                  )
           VALUES (batch_instance_x_job_param_seq.NEXTVAL,
                   p_batch_instance_sid,
                   (SELECT job_param_sid
                      FROM job_parameter
                     WHERE batch_job_sid IN (
                               SELECT batch_job_sid
                                 FROM batch_job
                                WHERE batch_job_name =
                                                      'REGREESION_TEST_CLAIMS')
                       AND job_param_name = 'DESCRIPTION'),
                   1, p_desc, 'A',
                   1, SYSDATE, 1, SYSDATE
                  );
   EXCEPTION
      WHEN OTHERS
      THEN
         -- dbms_output.put_line(sqlerrm);
         raise_application_error (SQLCODE, SQLERRM);
   END pr_batch_creation;

   PROCEDURE pr_cleanupclaim (p_batch_id IN NUMBER, p_created_by IN NUMBER)
   IS
      CURSOR c1
      IS
         SELECT reg_claim_header_sid
           FROM reg_prod_map
          WHERE reg_batch_id = p_batch_id;

      v_cnt       NUMBER           := 0;
      e_error     EXCEPTION;
      p_errcode   VARCHAR2 (200);
      p_errmsg    VARCHAR2 (32000);
   BEGIN
      FOR rec IN c1
      LOOP
         pk_claims_regression.pr_regression_cleanup_new
                                                   (rec.reg_claim_header_sid,
                                                    p_created_by,
                                                    p_errcode,
                                                    p_errmsg
                                                   );

         IF p_errcode = '0'
         THEN
            BEGIN
               DELETE FROM ht_clm_hdr_status
                     WHERE claim_header_sid = rec.reg_claim_header_sid;

               DELETE FROM ht_clm_ln_status
                     WHERE claim_line_sid IN (
                              SELECT claim_line_sid
                                FROM claim_line
                               WHERE claim_header_sid =
                                                     rec.reg_claim_header_sid);

               UPDATE claim_header
                  SET aplctn_status_cid = 50
                WHERE claim_header_sid = rec.reg_claim_header_sid
                  AND aplctn_status_cid = 51;

               UPDATE claim_line
                  SET aplctn_status_cid = 50
                WHERE claim_header_sid = rec.reg_claim_header_sid
                  AND aplctn_status_cid = 51;

               SELECT COUNT (*)
                 INTO v_cnt
                 FROM adjudication_queue
                WHERE claim_header_sid = rec.reg_claim_header_sid;

               IF v_cnt = 0
               THEN
                  INSERT INTO adjudication_queue
                              (claim_header_sid, batch_id, created_date,
                               mbr_sid, bi_prvdr_lctn_idntfr,
                               monitor_server_sid, process_date)
                     SELECT claim_header_sid, 8, SYSDATE, mbr_sid, NULL,
                            NULL, NULL
                       FROM claim_header
                      WHERE claim_header_sid = rec.reg_claim_header_sid;
               ELSE
                  UPDATE adjudication_queue
                     SET batch_id = 8
                   WHERE claim_header_sid = rec.reg_claim_header_sid;
               END IF;
            EXCEPTION
               WHEN OTHERS
               THEN
               p_errmsg:=rec.reg_claim_header_sid||'-'||p_errmsg;
                  --dbms_output.put_line(rec.reg_claim_header_sid||sqlerrm);
                  raise_application_error (p_errcode, p_errmsg);
            END;
         ELSE
            -- dbms_output.put_line(rec.reg_claim_header_sid);
            p_errmsg:=rec.reg_claim_header_sid||'-'||p_errmsg;
            
            RAISE e_error;
         END IF;
      END LOOP;
   EXCEPTION
      WHEN e_error
      THEN
         raise_application_error (p_errcode, p_errmsg);
      WHEN OTHERS
      THEN
         raise_application_error (SQLCODE, SQLERRM);
   END;

   PROCEDURE pr_poll_reg_new (
      p_created_by   IN       NUMBER,
      p_err_code     OUT      VARCHAR2,
      p_err_msg      OUT      VARCHAR2,
      p_batch_id     IN       NUMBER,
      p_reg_flag     IN       CHAR
   )
   IS
      CURSOR cr_reg_wip_detail (
         p_batch_sid   batch_instance.batch_instance_sid%TYPE
      -- p_run_number   NUMBER
      )
      IS
         SELECT rpm.*, ch.bsns_status_cid
           FROM reg_prod_map rpm, claim_header ch
          WHERE rpm.status_cid = 1                               --IN PROCESS
            AND ch.claim_header_sid = rpm.reg_claim_header_sid
            AND (   ch.bsns_status_cid IN
                                         (74, 82, 72, 71, 78, 80, 83, 84, 85)
                 OR ch.aplctn_status_cid = 75
                )
            AND rpm.reg_batch_id = p_batch_sid;

      --   AND rpm.run_number = p_run_number;
      CURSOR cr_reg_ad_detail (
         p_batch_sid   batch_instance.batch_instance_sid%TYPE
      --  p_run_number   NUMBER
      )
      IS
         SELECT rpm.*, ach.bsns_status_cid
           FROM reg_prod_map rpm, ad_claim_header ach
          WHERE rpm.status_cid = 1
            AND ach.claim_header_sid = rpm.reg_claim_header_sid
            AND ach.bsns_status_cid IN (74, 82, 72, 71, 78, 80, 83, 84, 85)
            AND rpm.reg_batch_id = p_batch_sid;

      --  AND rpm.run_number = p_run_number;

      /* line cursor*/
      CURSOR cr_adclmlndata (
         p_clm_hdr_sid   ad_claim_header.claim_header_sid%TYPE
      )
      IS
         SELECT claim_line_sid, claim_line_tcn
           FROM ad_claim_line
          WHERE claim_header_sid = p_clm_hdr_sid;

      CURSOR cr_wipclmlndata (
         p_clm_hdr_sid   claim_header.claim_header_sid%TYPE
      )
      IS
         SELECT claim_line_sid, claim_line_tcn
           FROM claim_line
          WHERE claim_header_sid = p_clm_hdr_sid;

      v_claim_header_sid    claim_header.claim_header_sid%TYPE;
      v_run_nmbr            NUMBER;

      TYPE t_batch_list IS TABLE OF NUMBER (15)
         INDEX BY BINARY_INTEGER;

      v_batch_list          t_batch_list;
      v_adwip_flag          CHAR (1);
      v_status_cid          status.status_cid%TYPE;
      v_err_code            VARCHAR2 (200);
      v_err_msg             VARCHAR2 (32767);
      e_procerror           EXCEPTION;
      v_process_cnt         NUMBER;
      v_clm_ln_sid          claim_line.claim_line_sid%TYPE;
      v_clm_ln_tcn          claim_line.claim_line_tcn%TYPE;
      i                     NUMBER                               := 0;
      v_regprod_data_flag   CHAR;
   BEGIN
   
   p_err_code :='0';
   p_err_msg:='Success';
      -- dbms_output.put_line('p_flag'||p_flag);
       -- dbms_output.put_line('p_flag'||p_flag);

      --UPDATE BATCH_INSTANCE
--      SET STATUS = p_flag ,
--      modified_by=p_created_by,
--      modified_date=sysdate
--      WHERE BATCH_INSTANCE_SID in (SELECT BATCH_INSTANCE_SID
--           FROM BATCH_INSTANCE
--          WHERE BATCH_JOB_SID =500000001  AND STATUS = 'W' and rownum < 11);

      -- dbms_output.put_line('Batch Number:' ||REC_BATCH.batch_instance_sid);
      IF p_reg_flag = 'B'
      THEN
         v_regprod_data_flag := 'I';
      ELSE
         v_regprod_data_flag := 'U';
      END IF;

      -- dbms_output.put_line('run Number:' || v_run_nmbr);
      FOR rec_clm_wip_hdr IN cr_reg_wip_detail (p_batch_id)    --, v_run_nmbr)
      LOOP
         SELECT NVL (MAX (run_number), 0)
           INTO v_run_nmbr
           FROM reg_prod_map
          WHERE reg_batch_id = p_batch_id
                AND reg_tcn = rec_clm_wip_hdr.reg_tcn;

         --dbms_output.put_line('inside first loop');

         /* BEGIN

         dbms_output.put_line('claim header sid:' || REC_CLM_HDR.REG_CLAIM_HEADER_SID);
         -- dbms_output.put_line('Business Status:' || REC_CLM_HDR.BSNS_STATUS_CID);


          UPDATE REG_PROD_MAP
          SET STATUS_CID=2,
           modified_by=p_created_by,
           modified_date=sysdate
          WHERE REG_CLAIM_HEADER_SID = REC_CLM_HDR.REG_CLAIM_HEADER_SID
          and REG_BATCH_ID =REC_CLM_HDR.REG_BATCH_ID
          and run_number=v_run_nmbr;--reg_header_sid

          EXCEPTION WHEN OTHERS THEN
          v_ERR_CODE:=SQLCODE;
          v_ERR_MSG:='Error while updating reg_prod_map';

          END ;*/

         /*  IF REC_CLM_HDR.BSNS_STATUS_CID IN ( 71,72,82,84 ) THEN

           V_ADWIP_FLAG:='A';

           ELSIF REC_CLM_HDR.BSNS_STATUS_CID in (74,70) THEN

           V_ADWIP_FLAG:='W';

           END IF;*/

         -- dbms_output.put_line('before header level compare:' || V_ADWIP_FLAG);

         --  dbms_output.put_line('INVOICE_TYPE_LKPCD:' || REC_CLM_wip_HDR.INVOICE_TYPE_LKPCD);
         pk_regression_fast_final1.pr_reg_prod_hdr_val
                                        (rec_clm_wip_hdr.reg_claim_header_sid,
                                         rec_clm_wip_hdr.reg_claim_header_sid,
                                         rec_clm_wip_hdr.reg_tcn,
                                         rec_clm_wip_hdr.reg_tcn,
                                         v_run_nmbr,
                                         rec_clm_wip_hdr.reg_batch_id,
                                         p_created_by,
                                         NULL,               --P_SRC_LINK    ,
                                         'W',
                                         v_regprod_data_flag,
                                         v_err_code,
                                         v_err_msg,
                                         rec_clm_wip_hdr.invoice_type_lkpcd,
                                         'N'
                                        );

         IF v_err_code <> '0'
         THEN
            -- dbms_output.put_line('after header procedure');
             --dbms_output.put_line('v_err_msg'||v_err_msg);
            RAISE e_procerror;
         END IF;

         -- dbms_output.put_line('after header level compare:' || V_ADWIP_FLAG);
         /*

           --  code for line
           IF V_ADWIP_FLAG = 'A' THEN
              -- dbms_output.put_line('open cursor:' ||  REC_CLM_HDR.REG_CLAIM_HEADER_SID);
               OPEN cr_adclmlndata(REC_CLM_HDR.REG_CLAIM_HEADER_SID);
             ELSIF V_ADWIP_FLAG = 'W' THEN
              --  dbms_output.put_line('open cursor:' ||  REC_CLM_HDR.REG_CLAIM_HEADER_SID);
               OPEN cr_wipclmlndata(REC_CLM_HDR.REG_CLAIM_HEADER_SID);
            END IF;




            loop
          --  dbms_output.put_line('in loop:');
              v_clm_ln_sid := null;
              v_clm_ln_tcn := null;
            IF V_ADWIP_FLAG = 'A' THEN

                  FETCH cr_adclmlndata
                   INTO   v_clm_ln_sid,v_clm_ln_tcn ;
                  EXIT WHEN cr_adclmlndata%NOTFOUND;
                ELSIF V_ADWIP_FLAG  = 'W' THEN
               --   dbms_output.put_line('in W' || V_ADWIP_FLAG);
                  FETCH cr_wipclmlndata
                   INTO   v_clm_ln_sid,v_clm_ln_tcn  ;
                 --  dbms_output.put_line('in lines sid' || v_clm_ln_sid || ':' ||v_clm_ln_tcn);
                  EXIT WHEN cr_wipclmlndata%NOTFOUND;
                END IF;

              */
               -- adding the code to habdle new line elements
         OPEN cr_wipclmlndata (rec_clm_wip_hdr.reg_claim_header_sid);

         LOOP
            FETCH cr_wipclmlndata
             INTO v_clm_ln_sid, v_clm_ln_tcn;

            --  dbms_output.put_line('in lines sid' || v_clm_ln_sid || ':' ||v_clm_ln_tcn);
            EXIT WHEN cr_wipclmlndata%NOTFOUND;
            pk_regression_fast_final1.pr_reg_prod_line_val_new
                                (rec_clm_wip_hdr.reg_claim_header_sid,
                                 v_clm_ln_sid, --rec_prod_line.claim_line_sid,
                                 rec_clm_wip_hdr.reg_claim_header_sid,
                                 v_clm_ln_sid,
                                 rec_clm_wip_hdr.reg_tcn,
                                 v_clm_ln_tcn,
                                 rec_clm_wip_hdr.reg_tcn,
                                 v_clm_ln_tcn,
                                 v_run_nmbr,
                                 rec_clm_wip_hdr.reg_batch_id,
                                 p_created_by,
                                 NULL,
                                 'W',
                                 v_regprod_data_flag,
                                 v_err_code,
                                 v_err_msg,
                                 rec_clm_wip_hdr.invoice_type_lkpcd,
                                 'N'
                                );

            IF v_err_code <> '0'
            THEN
               -- dbms_output.put_line('after line procedure');
               RAISE e_procerror;
            END IF;
         END LOOP;

         CLOSE cr_wipclmlndata;

                /*

         --  dbms_output.put_line('before line level compare:' || REC_CLM_HDR.REG_CLAIM_HEADER_SID);
             pk_Regression_fast.pr_reg_prod_line_val
                                         (REC_CLM_HDR.PROD_CLAIM_HEADER_SID,
                                          null,--rec_prod_line.claim_line_sid,
                                          REC_CLM_HDR.REG_CLAIM_HEADER_SID,
                                           v_clm_ln_sid,
                                          null,
                                          null,
                                          REC_CLM_HDR.reg_tcn,
                                          v_clm_ln_tcn ,
                                          v_run_nmbr,
                                          REC_CLM_HDR.REG_BATCH_ID,
                                          p_created_by,
                                          null,
                                          V_ADWIP_FLAG,
                                          'U',
                                          v_err_code,
                                          v_err_msg,
                                          REC_CLM_HDR.INVOICE_TYPE_LKPCD
                                         );

                       IF v_err_code <> '0'
                       THEN
                          RAISE e_procerror;
                       END IF;

           end loop;
         --  dbms_output.put_line('out line loop:' );

           IF V_ADWIP_FLAG = 'A' THEN
                 close cr_adclmlndata;
                  ELSIF V_ADWIP_FLAG  = 'W' THEN
                 close cr_wipclmlndata;

                END IF;

           */
         BEGIN
            UPDATE reg_prod_map
               SET status_cid = 3,
                   modified_by = p_created_by,
                   modified_date = SYSDATE
             WHERE reg_claim_header_sid = rec_clm_wip_hdr.reg_claim_header_sid
               AND reg_batch_id = rec_clm_wip_hdr.reg_batch_id
               AND run_number = v_run_nmbr;
         EXCEPTION
            WHEN OTHERS
            THEN
               v_err_code := SQLCODE;
               v_err_msg := SUBSTR (SQLERRM, 1, 200);
         END;
      -- COMMIT;
      END LOOP;

      FOR rec_clm_ad_hdr IN cr_reg_ad_detail (p_batch_id)      --, v_run_nmbr)
      LOOP
         SELECT NVL (MAX (run_number), 0)
           INTO v_run_nmbr
           FROM reg_prod_map
          WHERE reg_batch_id = p_batch_id AND reg_tcn = rec_clm_ad_hdr.reg_tcn;

         /* BEGIN

         -- dbms_output.put_line('claim header sid:' || REC_CLM_HDR.REG_CLAIM_HEADER_SID);
         -- dbms_output.put_line('Business Status:' || REC_CLM_HDR.BSNS_STATUS_CID);


          UPDATE REG_PROD_MAP
          SET STATUS_CID=2,
           modified_by=p_created_by,
           modified_date=sysdate
          WHERE REG_CLAIM_HEADER_SID = REC_CLM_HDR.REG_CLAIM_HEADER_SID
          and REG_BATCH_ID =REC_CLM_HDR.REG_BATCH_ID
          and run_number=v_run_nmbr;--reg_header_sid

          EXCEPTION WHEN OTHERS THEN
          v_ERR_CODE:=SQLCODE;
          v_ERR_MSG:='Error while updating reg_prod_map';

          END ;*/

         /*  IF REC_CLM_HDR.BSNS_STATUS_CID IN ( 71,72,82,84 ) THEN

           V_ADWIP_FLAG:='A';

           ELSIF REC_CLM_HDR.BSNS_STATUS_CID in (74,70) THEN

           V_ADWIP_FLAG:='W';

           END IF;*/

         --  dbms_output.put_line('before header level compare:' || V_ADWIP_FLAG);

         --  dbms_output.put_line('INVOICE_TYPE_LKPCD:' || REC_CLM_ad_HDR.INVOICE_TYPE_LKPCD);
         pk_regression_fast_final1.pr_reg_prod_hdr_val
                                         (rec_clm_ad_hdr.reg_claim_header_sid,
                                          rec_clm_ad_hdr.reg_claim_header_sid,
                                          rec_clm_ad_hdr.reg_tcn,
                                          rec_clm_ad_hdr.reg_tcn,
                                          v_run_nmbr,
                                          rec_clm_ad_hdr.reg_batch_id,
                                          p_created_by,
                                          NULL,              --P_SRC_LINK    ,
                                          'A',
                                          v_regprod_data_flag,
                                          v_err_code,
                                          v_err_msg,
                                          rec_clm_ad_hdr.invoice_type_lkpcd,
                                          'N'
                                         );

         IF v_err_code <> '0'
         THEN
            --dbms_output.put_line('after header ad  procedure');
            RAISE e_procerror;
         END IF;

         OPEN cr_adclmlndata (rec_clm_ad_hdr.reg_claim_header_sid);

         LOOP
            v_clm_ln_sid := NULL;
            v_clm_ln_tcn := NULL;

            FETCH cr_adclmlndata
             INTO v_clm_ln_sid, v_clm_ln_tcn;

            EXIT WHEN cr_adclmlndata%NOTFOUND;
            pk_regression_fast_final1.pr_reg_prod_line_val_new
                                (rec_clm_ad_hdr.reg_claim_header_sid,
                                 v_clm_ln_sid, --rec_prod_line.claim_line_sid,
                                 rec_clm_ad_hdr.reg_claim_header_sid,
                                 v_clm_ln_sid,
                                 rec_clm_ad_hdr.reg_tcn,
                                 v_clm_ln_tcn,
                                 rec_clm_ad_hdr.reg_tcn,
                                 v_clm_ln_tcn,
                                 v_run_nmbr,
                                 rec_clm_ad_hdr.reg_batch_id,
                                 p_created_by,
                                 NULL,
                                 'A',
                                 v_regprod_data_flag,
                                 v_err_code,
                                 v_err_msg,
                                 rec_clm_ad_hdr.invoice_type_lkpcd,
                                 'N'
                                );

            IF v_err_code <> '0'
            THEN
               -- dbms_output.put_line('after header line  procedure');
               RAISE e_procerror;
            END IF;
         END LOOP;

         CLOSE cr_adclmlndata;

         -- dbms_output.put_line('after header level compare:' || V_ADWIP_FLAG);
         /*

           --  code for line
           IF V_ADWIP_FLAG = 'A' THEN
              -- dbms_output.put_line('open cursor:' ||  REC_CLM_HDR.REG_CLAIM_HEADER_SID);
               OPEN cr_adclmlndata(REC_CLM_HDR.REG_CLAIM_HEADER_SID);
             ELSIF V_ADWIP_FLAG = 'W' THEN
              --  dbms_output.put_line('open cursor:' ||  REC_CLM_HDR.REG_CLAIM_HEADER_SID);
               OPEN cr_wipclmlndata(REC_CLM_HDR.REG_CLAIM_HEADER_SID);
            END IF;

            loop
          --  dbms_output.put_line('in loop:');
              v_clm_ln_sid := null;
              v_clm_ln_tcn := null;
            IF V_ADWIP_FLAG = 'A' THEN

                  FETCH cr_adclmlndata
                   INTO   v_clm_ln_sid,v_clm_ln_tcn ;
                  EXIT WHEN cr_adclmlndata%NOTFOUND;
                ELSIF V_ADWIP_FLAG  = 'W' THEN
               --   dbms_output.put_line('in W' || V_ADWIP_FLAG);
                  FETCH cr_wipclmlndata
                   INTO   v_clm_ln_sid,v_clm_ln_tcn  ;
                 --  dbms_output.put_line('in lines sid' || v_clm_ln_sid || ':' ||v_clm_ln_tcn);
                  EXIT WHEN cr_wipclmlndata%NOTFOUND;
                END IF;

         --  dbms_output.put_line('before line level compare:' || REC_CLM_HDR.REG_CLAIM_HEADER_SID);
             pk_Regression_fast.pr_reg_prod_line_val
                                         (REC_CLM_HDR.PROD_CLAIM_HEADER_SID,
                                          null,--rec_prod_line.claim_line_sid,
                                          REC_CLM_HDR.REG_CLAIM_HEADER_SID,
                                           v_clm_ln_sid,
                                          null,
                                          null,
                                          REC_CLM_HDR.reg_tcn,
                                          v_clm_ln_tcn ,
                                          v_run_nmbr,
                                          REC_CLM_HDR.REG_BATCH_ID,
                                          p_created_by,
                                          null,
                                          V_ADWIP_FLAG,
                                          'U',
                                          v_err_code,
                                          v_err_msg,
                                          REC_CLM_HDR.INVOICE_TYPE_LKPCD
                                         );

                       IF v_err_code <> '0'
                       THEN
                          RAISE e_procerror;
                       END IF;

           end loop;
         --  dbms_output.put_line('out line loop:' );

           IF V_ADWIP_FLAG = 'A' THEN
                 close cr_adclmlndata;
                  ELSIF V_ADWIP_FLAG  = 'W' THEN
                 close cr_wipclmlndata;

                END IF;

           */
         BEGIN
            UPDATE reg_prod_map
               SET status_cid = 3,
                   modified_by = p_created_by,
                   modified_date = SYSDATE
             WHERE reg_claim_header_sid = rec_clm_ad_hdr.reg_claim_header_sid
               AND reg_batch_id = rec_clm_ad_hdr.reg_batch_id
               AND run_number = v_run_nmbr;
         EXCEPTION
            WHEN OTHERS
            THEN
               v_err_code := SQLCODE;
               v_err_msg := SUBSTR (SQLERRM, 1, 200);
         END;
      --  COMMIT;
      END LOOP;

      IF p_reg_flag = 'A'
      THEN
         BEGIN
            SELECT COUNT (status_cid)
              INTO v_process_cnt
              FROM reg_prod_map
             WHERE reg_batch_id = p_batch_id
               AND status_type_cid = 84
               AND status_cid <> 3;
         --  dbms_output.put_line('Count of header not 3:' || v_process_cnt);
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_err_code := SQLCODE;
               v_err_msg := SUBSTR (SQLERRM, 1, 200);
         END;

         IF v_process_cnt > 0
         THEN
            BEGIN
               UPDATE batch_instance a
                  SET a.status = 'W',
                      modified_by = p_created_by,
                      modified_date = SYSDATE
                WHERE a.batch_instance_sid = p_batch_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_err_code := SQLCODE;
                  v_err_msg := SUBSTR (SQLERRM, 1, 200);
            END;
         ELSE
            BEGIN
               UPDATE batch_instance a
                  SET a.status = 'E',               -- waiting to be compared,
                      modified_by = p_created_by,
                      modified_date = SYSDATE
                WHERE a.batch_instance_sid = p_batch_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_err_code := SQLCODE;
                  v_err_msg := SUBSTR (SQLERRM, 1, 200);
            END;
         END IF;
      END IF;
   EXCEPTION
      WHEN e_procerror
      THEN
         p_err_code := v_err_code;
         p_err_msg := v_err_msg;
      WHEN OTHERS
      THEN
         -- dbms_output.put_line(v_err_code);
          --  dbms_output.put_line(v_err_msg);
         p_err_code := v_err_code;
         p_err_msg :=
               'Error in pr_poll_reg' || SUBSTR (SQLERRM, 1, 200)
               || v_err_msg;
         pr_reg_error_log (NULL,
                           NULL,
                           NULL,
                           v_clm_ln_sid,
                           NULL,
                           v_clm_ln_tcn,
                           NULL,
                           v_run_nmbr,
                           p_err_msg,
                           'REG_PROD_MAP,BATCH_INSTANCE',
                           p_created_by,
                           SYSDATE
                          );
   END;

   PROCEDURE pr_update_change_flag (
      p_batch_id     IN       NUMBER,
      p_created_by   IN       NUMBER,
      p_err_code     OUT      VARCHAR2,
      p_err_msg      OUT      VARCHAR2
   )
   IS
      CURSOR cr_batch
      IS
         SELECT *
           FROM batch_instance
          WHERE status = 'E'
            AND batch_job_sid IN (
                               SELECT batch_job_sid
                                 FROM batch_job
                                WHERE batch_job_name =
                                                      'REGREESION_TEST_CLAIMS')
            AND batch_instance_sid = p_batch_id;

      v_max_run_number       reg_prod_map.run_number%TYPE;

      -- p_created_by  number:=1;
      -- p_err_Code  varchar2(3200);
      -- p_err_msg  varchar2(32000);
      TYPE t_batch_ele IS TABLE OF NUMBER (30)
         INDEX BY BINARY_INTEGER;

      i                      NUMBER                                   := 0;
      v_batch_val            t_batch_ele;
      v_batch_instance_sid   batch_instance.batch_instance_sid%TYPE;
      v_prod_val             VARCHAR2 (32000);
      v_reg_val              VARCHAR2 (32000);
      string1                VARCHAR2 (32000);
      string2                VARCHAR2 (32000);
      v_change_col_value_p   VARCHAR2 (32000);
      v_change_col_value_r   VARCHAR2 (32000);
   BEGIN
      p_err_code := '0';
      p_err_msg := 'Success';

      --dbms_output.put_line('start of pr_compare procedure');
      FOR rec_batch IN cr_batch
      LOOP
         i := i + 1;
         v_batch_val (i) := rec_batch.batch_instance_sid;

--dbms_output.put_line('batch'||rec_batch.batch_instance_sid);
         UPDATE batch_instance
            SET status = 'I',
                modified_by = p_created_by,
                modified_date = SYSDATE
          WHERE batch_instance_sid = rec_batch.batch_instance_sid;
      END LOOP;

      IF v_batch_val.COUNT > 0
      THEN
         FOR i IN v_batch_val.FIRST .. v_batch_val.LAST
         LOOP
--dbms_output.put_line('inside the loop of batch in "I" status in batch instacne');
            v_batch_instance_sid := v_batch_val (i);

            /* SELECT MAX (run_number)
               INTO v_max_run_number
               FROM reg_prod_map
              WHERE reg_batch_id = v_batch_val (i);*/
            UPDATE reg_prod_compare
               SET col_value_change = NULL,
                   modified_by = p_created_by,
                   modified_date = SYSDATE
             WHERE                        --run_number = v_max_run_number  AND
                   reg_batch_id = v_batch_val (i);

--dbms_output.put_line('max run number'||  v_max_run_number);
            UPDATE reg_prod_compare
               SET col_value_change = 'Y',
                   modified_by = p_created_by,
                   modified_date = SYSDATE
             WHERE                             --run_number = v_max_run_number
                   --AND
                   reg_batch_id = v_batch_val (i)
               AND TRIM (NVL (change_col_value_p, '$')) <>
                                          TRIM (NVL (change_col_value_r, '$'));

--dbms_output.put_line('after update of reg_prod_compare ');
            UPDATE batch_instance
               SET status = 'D',
                   modified_by = p_created_by,
                   modified_date = SYSDATE
             WHERE batch_instance_sid = v_batch_val (i) AND status = 'I';

            --added by dhanalaxmi on 3/29/2008
            UPDATE reg_prod_compare a
               SET a.col_value_change = NULL
             WHERE                           --a.run_number = v_max_run_number
                   --AND
                   a.reg_batch_id = v_batch_val (i)
               AND a.processing_metadata_cid IN (211, 214);

            UPDATE reg_prod_compare a
               SET a.col_value_change = NULL
             WHERE                           --a.run_number = v_max_run_number
                   --AND
                   a.reg_batch_id = v_batch_val (i)
               AND (    TRIM (a.change_col_value_p) = 'Paid'
                    AND TRIM (a.change_col_value_r) = 'To Be Paid'
                   )
               AND a.processing_metadata_cid IN (277, 279);

            UPDATE reg_prod_compare a
               SET a.col_value_change = NULL
             WHERE
                   --a.run_number = v_max_run_number
                     --AND
                   a.reg_batch_id = v_batch_val (i)
               AND a.processing_metadata_cid IN (281);

            UPDATE reg_prod_compare
               SET col_value_change = NULL
             WHERE     reg_batch_id = v_batch_val (i)
                   AND processing_metadata_cid IN (217, 213)
                   AND (change_col_value_p = '0'
                        AND change_col_value_r IS NULL
                       )
                OR (change_col_value_p IS NULL AND change_col_value_r = '0');

            FOR rec IN (SELECT   *
                            FROM reg_prod_compare
                           WHERE reg_batch_id = v_batch_val (i)
                             AND processing_metadata_cid IN (272, 274)
                        ORDER BY reg_claim_header_sid)
            LOOP
               --   dbms_output.put_line('inside the 17 logic');
               IF rec.processing_metadata_cid IN (272, 274)
               THEN
                  v_prod_val := rec.change_col_value_p;
                  v_reg_val := rec.change_col_value_r;

                  SELECT REPLACE (v_prod_val, ',', ''',''')
                    INTO v_prod_val
                    FROM DUAL;

                  SELECT REPLACE (v_reg_val, ',', ''',''')
                    INTO v_reg_val
                    FROM DUAL;

                  string1 :=
                        'select fn_return_csv(cursor(select clm_error_nmbr from clm_error where clm_error_nmbr in ('''
                     || v_prod_val
                     || ''') order by clm_error_nmbr),'','') from dual';

                  EXECUTE IMMEDIATE string1
                               INTO v_change_col_value_p;

                  string2 :=
                        'select fn_return_csv(cursor(select clm_error_nmbr from clm_error where clm_error_nmbr in ('''
                     || v_reg_val
                     || ''') order by clm_error_nmbr),'','') from dual';

                  EXECUTE IMMEDIATE string2
                               INTO v_change_col_value_r;

                  IF    (   (INSTR (v_change_col_value_p,
                                    v_change_col_value_r) > 0
                            )
                         OR v_change_col_value_r IS NULL
                        )
                     OR (   (INSTR (v_change_col_value_r,
                                    v_change_col_value_p) > 0
                            )
                         OR v_change_col_value_p IS NULL
                        )
                  THEN
                     --          dbms_output.put_line('inside the 17 logic update');
                     UPDATE reg_prod_compare a
                        SET a.col_value_change = 'A'
                      WHERE
                            --a.run_number = v_max_run_number
                              --AND
                            a.reg_batch_id = v_batch_val (i)
                        AND a.processing_metadata_cid IN (272, 274)
                        AND NVL (change_col_value_p, '$') =
                                             NVL (rec.change_col_value_p, '$')
                        AND NVL (change_col_value_r, '$') =
                                             NVL (rec.change_col_value_r, '$')
                        AND reg_claim_header_sid = rec.reg_claim_header_sid
                        AND change_col_value_p LIKE '17%';
                  END IF;
               END IF;
            END LOOP;

            UPDATE reg_prod_compare a
               SET a.col_value_change = NULL
             WHERE a.reg_batch_id = v_batch_val (i)
               AND a.processing_metadata_cid IN (272, 274)
               AND change_col_value_p IS NULL
               AND change_col_value_r IS NULL;
--  dbms_output.put_line('after update of batch_instacne ');
         END LOOP;
      END IF;
--dbms_output.put_line('sjfhjs');
   EXCEPTION
      WHEN OTHERS
      THEN
         p_err_code := SQLCODE;
         p_err_msg :=
               'Error in procedure pk_reg_prod_compare.pr_comapre'
            || SUBSTR (SQLERRM, 1, 200);
   --raise_application_error (p_err_code, p_err_msg);
   END pr_update_change_flag;

   PROCEDURE pr_regression_cleanup_new (
      p_claim_header_sid   IN       claim_header.claim_header_sid%TYPE,
      p_created_by         IN       NUMBER,
      p_errcode            OUT      VARCHAR2,
      p_errmsg             OUT      VARCHAR2
   )
   IS
--
      v_claim_sbmsn_reason_lkpcd   claim_header.claim_submission_reason_lkpcd%TYPE;
      v_parent_tcn                 claim_header.parent_tcn%TYPE;
      v_old_claim_header_sid       claim_header.claim_header_sid%TYPE;
--v_last_run_detail_sid         CLM_HDR_DETAIL.last_run_detail_sid%TYPE;
      v_last_run_nmbr              NUMBER;
      v_cnt                        NUMBER;
      v_from_date                  DATE;
      v_bsns_status_cid            claim_header.bsns_status_cid%TYPE;
      v_last_run_number            claim_header.last_run_nmbr%TYPE;
      e_error                      EXCEPTION;
      v_parant_tcn                 ad_claim_header.parent_tcn%TYPE;
      v_tcn                        ad_claim_header.tcn%TYPE;
      v_wip_parant_tcn             claim_header.parent_tcn%TYPE;
      v_adj_bsns_status            claim_header.bsns_status_cid%TYPE;

--
      CURSOR c1 (p_claim_header_sid IN claim_header.claim_header_sid%TYPE)
      IS
         SELECT *
           FROM claim_line
          WHERE claim_header_sid = p_claim_header_sid;
--
   BEGIN
      p_errcode := '0';
      p_errmsg := 'Success';

      BEGIN
         SELECT bsns_status_cid
           INTO v_adj_bsns_status
           FROM ad_claim_header
          WHERE claim_header_sid = p_claim_header_sid;
      EXCEPTION
         WHEN OTHERS
         THEN
            BEGIN
               SELECT bsns_status_cid
                 INTO v_adj_bsns_status
                 FROM claim_header
                WHERE claim_header_sid = p_claim_header_sid;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_adj_bsns_status := NULL;
            END;
      END;

      -- dbms_output.put_line(v_adj_bsns_status);
      IF v_adj_bsns_status IS NOT NULL AND v_adj_bsns_status IN (80, 85)
      THEN
         BEGIN
            SELECT parent_tcn, tcn
              INTO v_parant_tcn, v_tcn
              FROM ad_claim_header
             WHERE claim_header_sid = p_claim_header_sid;
         EXCEPTION
            WHEN OTHERS
            THEN
               BEGIN
                  SELECT parent_tcn, tcn
                    INTO v_wip_parant_tcn, v_tcn
                    FROM claim_header
                   WHERE claim_header_sid = p_claim_header_sid;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_parant_tcn := NULL;
                     v_wip_parant_tcn := NULL;
               END;
         END;

         IF v_parent_tcn IS NOT NULL
         THEN
            UPDATE ad_claim_line
               SET bsns_status_cid = 81
             WHERE claim_header_sid IN (SELECT claim_header_sid
                                          FROM ad_claim_header
                                         WHERE tcn = v_parent_tcn);

            UPDATE ad_claim_header
               SET bsns_status_cid = 81
             WHERE tcn = v_parent_tcn;
         END IF;

         UPDATE ad_claim_line
            SET bsns_status_cid = 81
          WHERE claim_header_sid IN (SELECT claim_header_sid
                                       FROM ad_claim_header
                                      WHERE parent_tcn = v_tcn);

         UPDATE ad_claim_header
            SET bsns_status_cid = 81
          WHERE parent_tcn = v_tcn;

         UPDATE ad_claim_header
            SET parent_tcn = NULL,
                claim_submission_reason_lkpcd = '1'
          WHERE claim_header_sid = p_claim_header_sid
            AND bsns_status_cid IN (80, 85);

         IF v_wip_parant_tcn IS NOT NULL
         THEN
            UPDATE claim_line
               SET bsns_status_cid = 81
             WHERE claim_header_sid IN (SELECT claim_header_sid
                                          FROM claim_header
                                         WHERE tcn = v_wip_parant_tcn);

            UPDATE claim_header
               SET bsns_status_cid = 81
             WHERE tcn = v_wip_parant_tcn;
         END IF;

         UPDATE claim_line
            SET bsns_status_cid = 81
          WHERE claim_header_sid IN (SELECT claim_header_sid
                                       FROM claim_header
                                      WHERE parent_tcn = v_tcn);

         UPDATE claim_header
            SET bsns_status_cid = 81
          WHERE parent_tcn = v_tcn;

         UPDATE claim_header
            SET parent_tcn = NULL,
                claim_submission_reason_lkpcd = '1'
          WHERE claim_header_sid = p_claim_header_sid
            AND bsns_status_cid IN (80, 85);
      ELSIF v_adj_bsns_status IS NOT NULL AND v_adj_bsns_status IN (83, 84)
      THEN
         UPDATE ad_claim_line
            SET bsns_status_cid = 72
          WHERE claim_header_sid = p_claim_header_sid
            AND bsns_status_cid IN (83, 84);

         UPDATE ad_claim_header
            SET bsns_status_cid = 72
          WHERE claim_header_sid = p_claim_header_sid
            AND bsns_status_cid IN (83, 84);

         UPDATE claim_line
            SET bsns_status_cid = 72
          WHERE claim_header_sid = p_claim_header_sid
            AND bsns_status_cid IN (83, 84);

         UPDATE claim_header
            SET bsns_status_cid = 72
          WHERE claim_header_sid = p_claim_header_sid
            AND bsns_status_cid IN (83, 84);
      END IF;

      SELECT COUNT (1)
        INTO v_cnt
        FROM ad_claim_header
       WHERE claim_header_sid = p_claim_header_sid;

      IF v_cnt > 0
      THEN
         BEGIN
            SELECT bsns_status_cid
              INTO v_bsns_status_cid
              FROM ad_claim_header
             WHERE claim_header_sid = p_claim_header_sid
               AND bsns_status_cid IN
                                     (74, 75, 82, 72, 71, 78, 80, 83, 84, 85);
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_bsns_status_cid := NULL;
         END;

         IF v_bsns_status_cid IN (74, 75, 82, 72, 71, 78, 80, 83, 84, 85)
         THEN
            DBMS_OUTPUT.put_line ('paid/denied claims');

            SELECT COUNT (*)
              INTO v_cnt
              FROM ad_claim_header
             WHERE claim_header_sid = p_claim_header_sid;

            IF v_cnt > 0
            THEN
               pk_reprocessclaim.pr_reprocessclaim (p_claim_header_sid,
                                                    'N',
                                                    'REG',
                                                    p_created_by,
                                                    p_errcode,
                                                    p_errmsg
                                                   );

               IF p_errcode = '0'
               THEN
                  UPDATE adjudication_queue
                     SET batch_id = 9999
                   WHERE claim_header_sid = p_claim_header_sid;
               ELSE
                  RAISE e_error;
               END IF;
            END IF;
         END IF;
      ELSE
         BEGIN
            SELECT bsns_status_cid
              INTO v_bsns_status_cid
              FROM claim_header
             WHERE claim_header_sid = p_claim_header_sid
               AND bsns_status_cid IN
                                     (75, 74, 82, 72, 71, 78, 80, 83, 84, 85);
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_bsns_status_cid := NULL;
         END;

         IF v_bsns_status_cid IN (75, 74, 82, 72, 71, 78, 80, 83, 84, 85)
         THEN
            DBMS_OUTPUT.put_line ('suspended claims');
            pk_manageclaim.pr_resolveclaim (p_claim_header_sid,
                                            NULL,
                                            'N',
                                            'N',
                                            'N',
                                            p_created_by,
                                            p_errcode,
                                            p_errmsg
                                           );

            IF p_errcode = '0'
            THEN
               UPDATE adjudication_queue
                  SET batch_id = 9999
                WHERE claim_header_sid = p_claim_header_sid;
            ELSE
               RAISE e_error;
            END IF;
         END IF;
      END IF;

      BEGIN
         SELECT a.last_run_nmbr
           INTO v_last_run_number
           FROM claim_header a
          WHERE a.claim_header_sid = p_claim_header_sid;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            v_last_run_number := NULL;       --- if run_number is 0 then what
      END;

      IF v_last_run_number IS NOT NULL
      THEN
         SELECT COUNT (*)
           INTO v_cnt
           FROM clm_ln_x_fund_dtl clxfd, claim_line cl
          WHERE clxfd.claim_line_sid = cl.claim_line_sid
            AND cl.claim_header_sid = p_claim_header_sid;

         IF v_cnt <> 0
         THEN
            p_errcode := '0';
            p_errmsg := 'Success';
            RETURN;
         END IF;

         IF v_last_run_number > 0
         THEN
            UPDATE claim_header
               SET last_run_nmbr = v_last_run_number - 1
             WHERE claim_header_sid = p_claim_header_sid;

            UPDATE claim_line
               SET last_run_nmbr = v_last_run_number - 1
             WHERE claim_header_sid = p_claim_header_sid;
         END IF;

         DELETE FROM clm_hdr_run_error_asgnd_person
               WHERE clm_hdr_run_error_sid IN (
                        SELECT clm_hdr_run_error_sid
                          FROM clm_hdr_run_error
                         WHERE claim_header_sid = p_claim_header_sid
                           AND run_nmbr = v_last_run_number);

         DELETE FROM clm_hdr_run_error
               WHERE claim_header_sid = p_claim_header_sid
                 AND run_nmbr = v_last_run_number;

         DELETE FROM ht_clm_hdr_rsltn_change_log
               WHERE claim_header_sid = p_claim_header_sid
                 AND run_nmbr = v_last_run_number;

         FOR c1_rec IN c1 (p_claim_header_sid)
         LOOP
            /*    DELETE FROM clm_ln_x_indicator
                      WHERE derived_qlfr = 'D'
                        AND claim_line_sid = c1_rec.claim_line_sid;*/
            DELETE FROM clm_ln_run_error_asgnd_person
                  WHERE clm_ln_run_error_sid IN (
                           SELECT clm_ln_run_error_sid
                             FROM clm_ln_run_error
                            WHERE claim_line_sid = c1_rec.claim_line_sid
                              AND run_nmbr = v_last_run_number);

            DELETE FROM clm_ln_run_error
                  WHERE claim_line_sid = c1_rec.claim_line_sid
                    AND run_nmbr = v_last_run_number;

            DELETE FROM ht_clm_ln_rsltn_change_log
                  WHERE claim_line_sid = c1_rec.claim_line_sid
                    AND run_nmbr = v_last_run_number;

            DELETE FROM clm_ln_conflict
                  WHERE claim_line_sid = c1_rec.claim_line_sid
                    AND run_nmbr = v_last_run_number;

            DELETE FROM mbr_cost_of_care_utlztn
                  WHERE claim_line_sid = c1_rec.claim_line_sid;
         END LOOP;

         DELETE FROM clm_ln_amount
               WHERE claim_line_sid IN (
                                   SELECT claim_line_sid
                                     FROM claim_line
                                    WHERE claim_header_sid =
                                                            p_claim_header_sid)
                 AND amount_type_lkpcd = 'PP';

         DELETE FROM clm_hdr_conflict
               WHERE claim_header_sid = p_claim_header_sid
                 AND run_nmbr = v_last_run_number;

         DELETE FROM pa_rqst_prcdr_utilization
               WHERE claim_line_sid IN (
                                SELECT cl.claim_line_sid
                                  FROM claim_line cl
                                 WHERE cl.claim_header_sid =
                                                            p_claim_header_sid);
      END IF;
   EXCEPTION
      WHEN e_error
      THEN
         p_errcode := p_errcode;
         p_errmsg := p_errmsg || p_claim_header_sid;
      WHEN OTHERS
      THEN
         p_errcode := SQLCODE;
         p_errmsg := SUBSTR (SQLERRM, 1, 200);
   END;
END pk_claims_regression;
/
