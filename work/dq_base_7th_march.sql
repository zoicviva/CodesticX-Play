REPLACE PROCEDURE logistics_user.SP_LOAD_EM_CARRIER_DQ_RAW_FILE	   (IN in_batch_sk INTEGER ,OUT lv_out_status VARCHAR(10))
SQL SECURITY INVOKER







BEGIN
		DECLARE			lv_msg						VARCHAR(2000);
		DECLARE			ln_session_id				DECIMAL(30,0);
		DECLARE			lv_sql_state				VARCHAR(5);
		DECLARE			ln_sql_code					DECIMAL(20);
		DECLARE			lv_err_component			VARCHAR(100)	DEFAULT 'SP_LOAD_EM_CARRIER_DQ_RAW_FILE' ;
		
		
		DECLARE			lv_start_etl_batch_sk		INTEGER;
		DECLARE			lv_end_etl_batch_sk			INTEGER;
		DECLARE			lv_status					CHAR(1);

		
		DECLARE			ld_start_dt					DATE ;
		DECLARE			ld_end_dt					DATE ;
		DECLARE			ln_monthly_wk				INTEGER;
		DECLARE			ln_quarterly_wk				INTEGER;
		DECLARE			ld_month_start_dt			DATE ;
		DECLARE			ld_quarter_start_dt			DATE ;
		DECLARE			ld_month_end_dt				DATE ;
		DECLARE			ld_quarter_end_dt			DATE ;
		DECLARE			lv_fiscal_year_week_name	VARCHAR(30);
		
		
		
		DECLARE			ld_wk_del_dt				DATE ;
		DECLARE			ld_month_del_dt				DATE ;
		DECLARE			ld_qtr_del_dt				DATE ;
		DECLARE			lv_delete_year_week_name	VARCHAR(30);
		
		
		

		DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			SET lv_sql_state  = SQLSTATE;
			SET ln_sql_code	  = SQLCODE;
			SET lv_out_status = 'FAILED';
			ROLLBACK;
			SET  lv_msg = 'Error IN : ' || lv_msg || ' - SQLSTATE: ' || lv_sql_state || ', SQLCODE: ' || CAST(ln_sql_code AS CHAR(5)) || 'Session Id' || ln_session_id ;
			CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
		END;


		SELECT SESSION INTO ln_session_id;


		SET lv_msg = 'starting in Session Id '||' in Session Id ' || ln_session_id || ' and in_batch_sk ' || in_batch_sk ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		
		DELETE FROM logistics_app.wrk_em_dq_shipments;
		DELETE FROM logistics_app.wrk_em_dq_fiscal_dt_qualifier ;
		DELETE FROM logistics_app.wrk_em_dq_fiscal_dates ;
		DELETE FROM logistics_app.wrk_em_dq_event_latency_dtls ;
		DELETE FROM logistics_app.wrk_em_dq_event_uplift_dtls;
		DELETE FROM logistics_app.wrk_em_dq_doc_id_info;
		DELETE FROM logistics_app.wrk_em_carrier_dq_event_dtls;
		DELETE FROM logistics_app.wrk_em_dq_uplift_ind; 
		
		
		
		

		SELECT MAX(fiscal_dt) , MIN(fiscal_dt) INTO ld_end_dt , ld_start_dt FROM logistics.fiscal_day WHERE rolling_week = -1 AND region_cd='PAC';
		
		
		SELECT MAX(std_week_begin_dt)  INTO ld_wk_del_dt FROM logistics.fiscal_day WHERE rolling_week = -13 AND region_cd='PAC';
		
		
		SELECT MAX(std_period_begin_dt) INTO ld_month_del_dt FROM logistics.fiscal_day WHERE rolling_period = -13 AND region_cd='PAC';
		
		
		SELECT MAX(std_quarter_begin_dt) INTO ld_qtr_del_dt FROM logistics.fiscal_day WHERE rolling_quarter = -5 AND region_cd='PAC';
		
		
		SELECT 
			week_in_fiscal_period 
			,week_in_fiscal_quarter 
			,std_period_begin_dt
			,std_quarter_begin_dt 
			,std_period_end_dt 
			,std_quarter_end_dt  
			,std_Fiscal_Year_Week_Name
		INTO 
			ln_monthly_wk 
			,ln_quarterly_wk 
			,ld_month_start_dt 
			,ld_quarter_start_dt 
			,ld_month_end_dt
			,ld_quarter_end_dt 
			,lv_fiscal_year_week_name
		FROM 
			master.fiscal_calendar_cur 
		WHERE fiscal_dt=ld_end_dt;




		SET lv_msg = 'Inserting into wrk_em_dq_shipments for date range : '|| ld_start_dt ||' '|| ld_end_dt;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		INSERT INTO logistics_app.wrk_em_dq_shipments
		(
			shipment_id ,
			event_handler_nr
		)
		SELECT
			shipment_id ,
			event_handler_nr
		FROM 
			logistics.em_shipment_event
		WHERE  
		(
			(	
				eventdate_gmt BETWEEN ld_start_dt AND ld_end_dt AND 
				event_date BETWEEN ld_start_dt - 2 AND ld_end_dt + 2
			) 
			OR b2b_received_ts(DATE) BETWEEN ld_start_dt AND ld_end_dt 
		) 
			AND  (duplicate_ind IS NULL or duplicate_ind = 'N')
			AND rptg_cd = 'Y'
			AND event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list)
		GROUP BY 1,2 ;





		COLLECT STATS ON  logistics_app.wrk_em_dq_shipments;






		SET  lv_msg = 'Inserting into work table wrk_em_dq_doc_id_info' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));


		INSERT INTO logistics_app.wrk_em_dq_doc_id_info
		(
			shipment_id
			,event_handler_nr
			,sales_order_nr
			,delivery_nr
			,po_nr
			,doc_type_cd
		)
		SELECT 
			wrk1.shipment_id
			,wrk1.event_handler_nr
			,MAX(
				CASE 
					WHEN sdr.order_categ_cd='PO' THEN sdr.po_nr 
					WHEN sdr.order_categ_cd='DN' THEN sdr.delivery_nr
					WHEN sdr.order_categ_cd='STO' THEN sdr.delivery_nr
					ELSE ede.sales_order_nr 
				END
				)
			,MAX(sdr.delivery_nr)
			,MAX(sdr.po_nr)
			,MAX(ede.doc_type_cd)
		FROM 
			logistics_app.wrk_em_dq_shipments wrk1
			LEFT OUTER JOIN supply_chain.shipment_document_ref_cur sdr 
				ON ( sdr.shipment_id= wrk1.shipment_id AND sdr.event_handler_nr=wrk1.event_handler_nr AND sdr.rptg_cd= 'Y' AND sdr.delivery_nr <> '$')
			LEFT OUTER JOIN logistics.em_delivery_extn ede 
				ON (ede.Delivery_nr=sdr.Delivery_nr AND ede.delivery_item_nr=sdr.delivery_item_nr) 
		GROUP BY  1,2 ;


		COLLECT STATS ON  logistics_app.wrk_em_dq_doc_id_info;





		SET  lv_msg = 'Inserting into work table wrk_em_carrier_dq_event_dtls for PAC and AMR' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		INSERT INTO logistics_app.wrk_em_carrier_dq_event_dtls 
		(
			event_handler_nr
			,shipment_id
			,event_msg_nr
			,event_cd
			,event_name
			,event_phase_cd
			,region_cd
			,sender_nr
			,sender_name
			,event_local_dt
			,b2b_received_ts
			,b2b_received_dt
			,loc_1_desc
			,orig_scac_cd
			,parent_scac_cd
			,ship_to_country_cd
			,event_gmt_dt
			,event_gmt_ts
			,event_rptg_gmt_ts
			,event_dt
			,delivery_sched_local_dt
			,all_tracking_nr
			,uplift_reqd_ind
			,scac_first_occurrence_ind 
			,scac_last_occurrence_ind
			,reason_cd			
	    ,reason_desc		    
		)
		SELECT 
			 ese.event_handler_nr 
			,ese.shipment_id
			,ese.event_msg_nr
			,ese.event_cd
			,ese.event_name
			,ese.event_phase_cd
			,ese.region_cd
			,COALESCE(ese.sender_nr , ' ')
			,ese.sender_name
			,ese.event_local_ts(Date)
			,ese.b2b_received_ts
			,ese.b2b_received_ts(Date)
			,COALESCE(ese.loc_1_desc , ese.loc_desc )
			,COALESCE(ese.orig_scac_cd,ese.parent_scac_cd)
			,COALESCE(ese.parent_scac_cd,ese.orig_scac_cd)
			,ese.ship_to_country_cd
			,ese.eventdate_gmt
			,ese.event_gmt_ts
			,ese.event_rptg_gmt_ts
			,ese.event_date
			,esm.delivery_sched_local_dt
			,esm.all_tracking_nr
			,'N'
			,CASE WHEN 
					(
					ROW_NUMBER() OVER ( PARTITION BY ese.shipment_id ,ese.event_phase_cd,ese.event_name,COALESCE(ese.parent_scac_cd,ese.orig_scac_cd) 
										ORDER BY ese.b2b_received_ts,ese.event_gmt_ts,ese.event_msg_nr ASC
										)
					)=1 THEN 1 
					ELSE 0 
				END AS scac_first_occurrence_ind 
			,CASE WHEN 
					(
					ROW_NUMBER() OVER ( PARTITION BY ese.shipment_id ,ese.event_phase_cd,ese.event_name,COALESCE(ese.parent_scac_cd,ese.orig_scac_cd) 
										ORDER BY ese.b2b_received_ts DESC,ese.event_gmt_ts DESC,ese.event_msg_nr DESC
										)
					)=1 THEN 1 
					ELSE 0 
				END AS scac_last_occurrence_ind
			,ese.reason_cd			   
	    ,ese.reason_desc		       
		FROM 
			logistics_app.wrk_em_dq_shipments wrk1 
			INNER JOIN logistics.em_dq_shipment_event ese 
				ON (wrk1.shipment_id = ese.shipment_id AND wrk1.event_handler_nr = ese.event_handler_nr)
			LEFT OUTER JOIN logistics.em_shipment_multi_values esm 
				ON  (wrk1.shipment_id = esm.shipment_id AND wrk1.event_handler_nr = esm.event_handler_nr )
		WHERE ese.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list) AND ese.rptg_cd='Y'
		AND (ese.eventdate_gmt <= ld_end_dt OR ese.b2b_received_ts(DATE) <= ld_end_dt)
		AND ese.region_cd IN ('PAC','AMR')
		;

		
		
		SET  lv_msg = 'Inserting into work table wrk_em_carrier_dq_event_dtls for EURO' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		INSERT INTO logistics_app.wrk_em_carrier_dq_event_dtls 
		(
			event_handler_nr
			,shipment_id
			,event_msg_nr
			,event_cd
			,event_name
			,event_phase_cd
			,region_cd
			,sender_nr
			,sender_name
			,event_local_dt
			,b2b_received_ts
			,b2b_received_dt
			,loc_1_desc
			,orig_scac_cd
			,parent_scac_cd
			,ship_to_country_cd
			,event_gmt_dt
			,event_gmt_ts
			,event_rptg_gmt_ts
			,event_dt
			,delivery_sched_local_dt
			,all_tracking_nr
			,uplift_reqd_ind
			,scac_first_occurrence_ind 
			,scac_last_occurrence_ind
			,reason_cd			
	    ,reason_desc 
		)
		SELECT 
			 ese.event_handler_nr 
			,ese.shipment_id
			,ese.event_msg_nr
			,ese.event_cd
			,ese.event_name
			,ese.event_phase_cd
			,ese.region_cd
			,COALESCE(ese.sender_nr , ' ')
			,ese.sender_name
			,ese.event_local_ts(Date)
			,ese.b2b_received_ts
			,ese.b2b_received_ts(Date)
			,COALESCE(ese.loc_1_desc , ese.loc_desc )
			,COALESCE(ese.orig_scac_cd,ese.parent_scac_cd)
			,COALESCE(ese.parent_scac_cd,ese.orig_scac_cd)
			,ese.ship_to_country_cd
			,ese.eventdate_gmt
			,ese.event_gmt_ts
			,ese.event_rptg_gmt_ts
			,ese.event_date
			,esm.delivery_sched_local_dt
			,esm.all_tracking_nr
			,'N'
			,CASE WHEN (
				ROW_NUMBER() OVER (PARTITION BY ese.shipment_id ,ese.event_phase_cd,ese.event_name,COALESCE(ese.parent_scac_cd,ese.orig_scac_cd),COALESCE(ese.sender_nr,' ') 
									ORDER BY ese.b2b_received_ts,ese.event_gmt_ts,ese.event_msg_nr ASC
									)
						)=1 THEN 1 
					ELSE 0 
				END AS scac_first_occurrence_ind 
			,CASE WHEN (
				ROW_NUMBER() OVER (PARTITION BY ese.shipment_id ,ese.event_phase_cd,ese.event_name,COALESCE(ese.parent_scac_cd,ese.orig_scac_cd),COALESCE(ese.sender_nr,' ') 
									ORDER BY ese.b2b_received_ts DESC,ese.event_gmt_ts DESC,ese.event_msg_nr DESC
									)
						)=1 THEN 1 
					ELSE 0 
				END AS scac_last_occurrence_ind 
			,ese.reason_cd	
	    ,ese.reason_desc 
		FROM 
			logistics_app.wrk_em_dq_shipments wrk1 
			INNER JOIN logistics.em_dq_shipment_event ese 
				ON (wrk1.shipment_id = ese.shipment_id AND wrk1.event_handler_nr = ese.event_handler_nr)
			LEFT OUTER JOIN logistics.em_shipment_multi_values esm 
				ON  (wrk1.shipment_id = esm.shipment_id AND wrk1.event_handler_nr = esm.event_handler_nr )
		WHERE ese.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list) 
		AND ese.rptg_cd='Y'
		AND (ese.eventdate_gmt <= ld_end_dt OR ese.b2b_received_ts(DATE) <= ld_end_dt)
		AND ese.region_cd IN ('EURO')
		;
		
								
		
		SET  lv_msg = 'Inserting into work table wrk_em_carrier_dq_event_dtls for First Attempt' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));


		INSERT INTO logistics_app.wrk_em_carrier_dq_event_dtls 
		(
			event_handler_nr
			,shipment_id
			,event_msg_nr
			,event_cd
			,event_name
			,event_phase_cd
			,region_cd
			,sender_nr
			,sender_name
			,event_local_dt
			,b2b_received_ts
			,b2b_received_dt
			,loc_1_desc
			,orig_scac_cd
			,parent_scac_cd
			,ship_to_country_cd
			,event_gmt_dt
			,event_gmt_ts
			,event_rptg_gmt_ts
			,event_dt
			,delivery_sched_local_dt
			,all_tracking_nr
			,uplift_reqd_ind
			,scac_first_occurrence_ind 
			,scac_last_occurrence_ind
			,reason_cd
	    ,reason_desc
		)
		SELECT 
			 event_handler_nr 
			,shipment_id
			,event_msg_nr
			,event_cd
			,'First Attempt'					
			,event_phase_cd
			,region_cd
			,sender_nr
			,sender_name
			,event_local_dt
			,b2b_received_ts
			,b2b_received_dt
			,loc_1_desc
			,orig_scac_cd
			,parent_scac_cd
			,ship_to_country_cd
			,event_gmt_dt
			,event_gmt_ts
			,event_rptg_gmt_ts
			,event_dt
			,delivery_sched_local_dt
			,all_tracking_nr
			,'N'
			,CASE WHEN 
					(
					ROW_NUMBER() OVER ( PARTITION BY shipment_id ,event_phase_cd,event_name,COALESCE(parent_scac_cd,orig_scac_cd) 
										ORDER BY b2b_received_ts,event_gmt_ts,event_msg_nr ASC
										)
					)=1 THEN 1 
					ELSE 0 
				END AS scac_first_occurrence_ind 
			,CASE WHEN 
					(
					ROW_NUMBER() OVER ( PARTITION BY shipment_id ,event_phase_cd,event_name,COALESCE(parent_scac_cd,orig_scac_cd) 
										ORDER BY b2b_received_ts DESC,event_gmt_ts DESC,event_msg_nr DESC
										)
					)=1 THEN 1 
					ELSE 0 
				END AS scac_last_occurrence_ind
			,reason_cd
	    ,reason_desc
		FROM 
			logistics_app.wrk_em_carrier_dq_event_dtls 
		WHERE event_cd IN ('AP') AND reason_cd IN (SELECT reason_cd FROM logistics.em_carrier_reason WHERE region_cd IN ('PAC','AMR') AND region_status_desc='attempted')
		AND region_cd IN ('PAC','AMR')
		;

		INSERT INTO logistics_app.wrk_em_carrier_dq_event_dtls 
		(
			event_handler_nr
			,shipment_id
			,event_msg_nr
			,event_cd
			,event_name
			,event_phase_cd
			,region_cd
			,sender_nr
			,sender_name
			,event_local_dt
			,b2b_received_ts
			,b2b_received_dt
			,loc_1_desc
			,orig_scac_cd
			,parent_scac_cd
			,ship_to_country_cd
			,event_gmt_dt
			,event_gmt_ts
			,event_rptg_gmt_ts
			,event_dt
			,delivery_sched_local_dt
			,all_tracking_nr
			,uplift_reqd_ind
			,scac_first_occurrence_ind 
			,scac_last_occurrence_ind
			,reason_cd			
	    ,reason_desc 
		)
		SELECT 
			event_handler_nr
			,shipment_id
			,event_msg_nr
			,event_cd
			,'First Attempt'							
			,event_phase_cd
			,region_cd
			,sender_nr
			,sender_name
			,event_local_dt
			,b2b_received_ts
			,b2b_received_dt
			,loc_1_desc
			,orig_scac_cd
			,parent_scac_cd
			,ship_to_country_cd
			,event_gmt_dt
			,event_gmt_ts
			,event_rptg_gmt_ts
			,event_dt
			,delivery_sched_local_dt
			,all_tracking_nr
			,'N'
			,CASE WHEN (
				ROW_NUMBER() OVER (PARTITION BY shipment_id ,event_phase_cd,event_name,COALESCE(parent_scac_cd,orig_scac_cd),COALESCE(sender_nr,' ') 
									ORDER BY b2b_received_ts,event_gmt_ts,event_msg_nr ASC
									)
						)=1 THEN 1 
					ELSE 0 
				END AS scac_first_occurrence_ind 
			,CASE WHEN (
				ROW_NUMBER() OVER (PARTITION BY shipment_id ,event_phase_cd,event_name,COALESCE(parent_scac_cd,orig_scac_cd),COALESCE(sender_nr,' ') 
									ORDER BY b2b_received_ts DESC,event_gmt_ts DESC,event_msg_nr DESC
									)
						)=1 THEN 1 
					ELSE 0 
				END AS scac_last_occurrence_ind 
			,reason_cd  
	    ,reason_desc 
		FROM 
			logistics_app.wrk_em_carrier_dq_event_dtls 
		WHERE event_cd IN ('AP') AND reason_cd IN (SELECT reason_cd FROM logistics.em_carrier_reason WHERE region_cd IN ('EURO') AND region_status_desc='attempted')
		AND region_cd IN ('EURO')
		;
		
		 
		SET  lv_msg = 'Deleting from wrk_em_carrier_dq_event_dtls for event cd appointment requested of scac : DHLG, EMSY, FDE, LASG, LOGD, RAC, RPSI, TNT, UPSN ' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));


		DELETE FROM logistics_app.wrk_em_carrier_dq_event_dtls
		WHERE parent_scac_cd IN ('DHLG', 'EMSY', 'FDE', 'LASG', 'LOGD', 'RAC', 'RPSI', 'TNT', 'UPSN')
		AND event_cd IN ('RL','RB')
		AND region_cd IN ('AMR'); 
		 
		

		COLLECT STATS ON  logistics_app.wrk_em_carrier_dq_event_dtls ;


 

		SET  lv_msg = 'Inserting into work table wrk_em_dq_uplift_ind for marking uflift' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		
		INSERT INTO logistics_app.wrk_em_dq_uplift_ind
		(
			event_handler_nr
			,shipment_id
			,parent_scac_cd
			,sender_nr
			,event_ind
		)
		SELECT 
			event_handler_nr
			,shipment_id
			,parent_scac_cd
			,sender_nr
			,CASE WHEN (ROW_NUMBER() OVER (PARTITION BY wrk.shipment_id , wrk.event_handler_nr 
									ORDER BY wrk.b2b_received_ts ASC,wrk.event_gmt_ts ASC,wrk.event_msg_nr ASC
									) = 1
						) THEN 1 
					ELSE 0 END AS rnk
		FROM 
			logistics_app.wrk_em_carrier_dq_event_dtls wrk
		WHERE 
			event_cd IN ('J1' , 'XB' ) 
			
			
		QUALIFY rnk=1
		; 
		
		

		COLLECT STATS ON  logistics_app.wrk_em_dq_uplift_ind ;







		SET  lv_msg = 'Inserting into work table wrk_em_dq_fiscal_dt_qualifier' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		INSERT INTO logistics_app.wrk_em_dq_fiscal_dt_qualifier 
		(
			event_handler_nr,
			shipment_id,
			orig_scac_cd,
			parent_scac_cd,
			sender_nr ,
			event_cd,
			event_gmt_dt,
			delivery_sched_local_dt,
			region_cd
		)
		SELECT 
			wrk3.event_handler_nr ,
			wrk3.shipment_id,
			wrk3.orig_scac_cd,
			wrk3.parent_scac_cd,
			wrk3.sender_nr ,
			wrk3.event_cd,
			wrk3.event_gmt_dt,
			wrk3.delivery_sched_local_dt,
			wrk3.region_cd
		FROM 
			logistics_app.wrk_em_carrier_dq_event_dtls wrk3
		WHERE	(
					(	
						wrk3.scac_first_occurrence_ind = 1 
						AND wrk3.event_cd IN  (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE occurrence_ind='F') 
					) 
					OR (
					wrk3.scac_last_occurrence_ind = 1 AND wrk3.event_cd IN	(SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE occurrence_ind='L') 
					)
				)
		;




		COLLECT STATS ON  logistics_app.wrk_em_dq_fiscal_dt_qualifier ;




		SET  lv_msg = 'Inserting into work table WRK_em_dq_fiscl_dt for PAC and AMR' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		INSERT INTO logistics_app.wrk_em_dq_fiscal_dates 
		(
			shipment_id
			,event_handler_nr
			,parent_scac_cd
			,fiscal_dt
		)
		SELECT 
			a.shipment_id, 
			a.event_handler_nr, 
			a.parent_scac_cd , 
			COALESCE(
				COALESCE(b.event_gmt_dt, b2.event_gmt_dt), 
				CASE 
					WHEN (c.event_gmt_dt IS NULL AND e.event_gmt_dt IS NULL) OR (d.event_gmt_dt > COALESCE(c.event_gmt_dt,e.event_gmt_dt) AND d.event_gmt_dt > COALESCE(e.event_gmt_dt,c.event_gmt_dt) ) THEN d.Event_Gmt_Dt 
					WHEN (e.event_gmt_dt > c.event_gmt_dt) THEN e.event_gmt_dt
					ELSE c.event_gmt_dt END, a.delivery_sched_local_dt
					) AS fiscal_dt	
		FROM 
			logistics_app.wrk_em_dq_fiscal_dt_qualifier  a
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dt_qualifier  b 
				ON a.shipment_id = b.shipment_id AND a.event_handler_nr	 = b.event_handler_nr  AND a.parent_scac_cd = b.parent_scac_cd AND b.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE priority_val=1)
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dt_qualifier  b2 
				ON a.shipment_id = b2.shipment_id AND a.event_handler_nr  = b2.event_handler_nr	 AND a.parent_scac_cd = b2.parent_scac_cd AND b2.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE priority_val=2)
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dt_qualifier  c 
				ON a.shipment_id = c.shipment_id AND a.event_handler_nr	 = c.event_handler_nr  AND a.parent_scac_cd = c.parent_scac_cd AND c.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE priority_val=3)
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dt_qualifier  d 
				ON a.shipment_id = d.shipment_id AND a.event_handler_nr	 = d.event_handler_nr  AND a.parent_scac_cd = d.parent_scac_cd AND d.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE priority_val=4) 
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dt_qualifier  e 
				ON a.shipment_id = d.shipment_id AND a.event_handler_nr	 = e.event_handler_nr  AND a.parent_scac_cd = e.parent_scac_cd AND e.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE priority_val=5)
		WHERE a.region_cd IN ('PAC','AMR')
		GROUP BY  1,2,3,4 ;
		
		SET  lv_msg = 'Inserting into work table WRK_em_dq_fiscl_dt for EURO' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		INSERT INTO logistics_app.wrk_em_dq_fiscal_dates 
		(
			shipment_id
			,event_handler_nr
			,parent_scac_cd
			,sender_nr
			,fiscal_dt
		)
		SELECT 
			a.shipment_id, 
			a.event_handler_nr, 
			a.parent_scac_cd ,
			a.sender_nr, 
			COALESCE(
				COALESCE(b.event_gmt_dt, b2.event_gmt_dt), 
				CASE 
					WHEN (c.event_gmt_dt IS NULL AND e.event_gmt_dt IS NULL) OR (d.event_gmt_dt > COALESCE(c.event_gmt_dt,e.event_gmt_dt) AND d.event_gmt_dt > COALESCE(e.event_gmt_dt,c.event_gmt_dt) ) THEN d.Event_Gmt_Dt 
					WHEN (e.event_gmt_dt > c.event_gmt_dt) THEN e.event_gmt_dt
					ELSE c.event_gmt_dt END, a.delivery_sched_local_dt
					) AS fiscal_dt	
		FROM 
			logistics_app.wrk_em_dq_fiscal_dt_qualifier  a
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dt_qualifier  b 
				ON a.shipment_id = b.shipment_id AND a.event_handler_nr	 = b.event_handler_nr  AND a.parent_scac_cd = b.parent_scac_cd AND a.sender_nr=b.sender_nr	AND b.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE priority_val=1)
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dt_qualifier  b2 
				ON a.shipment_id = b2.shipment_id AND a.event_handler_nr  = b2.event_handler_nr	 AND a.parent_scac_cd = b2.parent_scac_cd AND a.sender_nr=b2.sender_nr	AND b2.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE priority_val=2)
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dt_qualifier  c 
				ON a.shipment_id = c.shipment_id AND a.event_handler_nr	 = c.event_handler_nr  AND a.parent_scac_cd = c.parent_scac_cd AND a.sender_nr=c.sender_nr AND c.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE priority_val=3)
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dt_qualifier  d 
				ON a.shipment_id = d.shipment_id AND a.event_handler_nr	 = d.event_handler_nr  AND a.parent_scac_cd = d.parent_scac_cd AND a.sender_nr=d.sender_nr	AND d.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE priority_val=4)
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dt_qualifier  e 
				ON a.shipment_id = d.shipment_id AND a.event_handler_nr	 = e.event_handler_nr  AND a.parent_scac_cd = e.parent_scac_cd AND a.sender_nr=e.sender_nr	AND e.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE priority_val=5)
		WHERE a.region_cd IN ('EURO')
		GROUP BY  1,2,3,4,5 ;


		COLLECT STATS ON  logistics_app.wrk_em_dq_fiscal_dates ;







		SET  lv_msg = 'Inserting into work table wrk_em_dq_event_latency_dtls for PAC and AMR' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		INSERT INTO logistics_app.wrk_em_dq_event_latency_dtls
		(
			event_handler_nr 
			,shipment_id
			,region_cd
			,sender_nr
			,sender_name 
			,event_local_dt
			,b2b_received_ts
			,b2b_received_dt
			,loc_1_desc
			,event_msg_nr
			,ship_to_country_cd
			,event_cd
			,event_phase_cd
			,event_name
			,diff_in_hour_val
			,diff_in_sec_val
			,orig_scac_cd
			,parent_scac_cd
			,fiscal_dt
			,event_gmt_dt
			,event_gmt_ts
			,event_rptg_gmt_ts
			,delivery_sched_local_dt
			,all_tracking_nr
			,uplift_reqd_ind
			,reason_cd			    
	    ,reason_desc			
	    ,scac_first_occurrence_ind			
	    ,scac_last_occurrence_ind		
		)
		SELECT 
			wrk3.event_handler_nr 
			,wrk3.shipment_id
			,wrk3.region_cd
			,wrk3.sender_nr 
			,wrk3.sender_name
			,wrk3.event_local_dt
			,wrk3.b2b_received_ts
			,wrk3.b2b_received_dt
			,wrk3.loc_1_desc
			,wrk3.event_msg_nr
			,wrk3.ship_to_country_cd
			,wrk3.event_cd	 
			,wrk3.event_phase_cd
			,wrk3.event_name
			,
			CASE WHEN wrk3.event_gmt_dt > '2000-01-01' AND wrk3.event_gmt_dt < '2030-01-01' 
			AND wrk3.b2b_received_dt > '2000-01-01' AND wrk3.b2b_received_dt < '2030-01-01'	 THEN	
			(
			(EXTRACT(DAY FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))*24.00) +
			(EXTRACT(HOUR FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))) +
			(EXTRACT(MINUTE FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))/60.00) + 
			(EXTRACT(SECOND FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))/3600.00)
			) ELSE NULL END AS diff_in_hour_val
			,
			CASE WHEN wrk3.event_gmt_dt > '2000-01-01' AND wrk3.event_gmt_dt < '2030-01-01' 
			AND wrk3.b2b_received_dt > '2000-01-01' AND wrk3.b2b_received_dt < '2030-01-01'	 THEN	
			(
			(EXTRACT(DAY FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))*24.00) +
			(EXTRACT(HOUR FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))) +
			(EXTRACT(MINUTE FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))/60.00) + 
			(EXTRACT(SECOND FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))/3600.00)
			) ELSE NULL END AS Diff_In_Sec_Val
			,wrk3.orig_scac_cd
			,wrk3.parent_scac_cd
			,wrk5.fiscal_dt
			,wrk3.event_gmt_dt
			,wrk3.event_gmt_ts
			,wrk3.event_rptg_gmt_ts
			,wrk3.delivery_sched_local_dt
			,wrk3.all_tracking_nr
			,wrk3.uplift_reqd_ind
			,wrk3.reason_cd	 
	    ,wrk3.reason_desc 
	    ,wrk3.scac_first_occurrence_ind			
	    ,wrk3.scac_last_occurrence_ind		
		FROM 
			logistics_app.wrk_em_carrier_dq_event_dtls wrk3 
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dates  wrk5
				ON (wrk3.shipment_id = wrk5.shipment_id AND wrk3.event_handler_nr=wrk5.event_handler_nr AND wrk3.parent_scac_cd = wrk5.parent_scac_cd	)
		WHERE	(
					(
					wrk3.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE occurrence_ind IN ('F','B')) AND wrk3.scac_first_occurrence_ind=1 
					)
					OR (wrk3.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE occurrence_ind='L') AND wrk3.scac_last_occurrence_ind=1 )
				)
		AND wrk3.region_cd IN ('PAC','AMR')
		;

		SET  lv_msg = 'Inserting into work table wrk_em_dq_event_latency_dtls for EURO' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		INSERT INTO logistics_app.wrk_em_dq_event_latency_dtls
		(
			event_handler_nr 
			,shipment_id
			,region_cd
			,sender_nr
			,sender_name 
			,event_local_dt
			,b2b_received_ts
			,b2b_received_dt
			,loc_1_desc
			,event_msg_nr
			,ship_to_country_cd
			,event_cd
			,event_phase_cd
			,event_name
			,diff_in_hour_val
			,diff_in_sec_val
			,orig_scac_cd
			,parent_scac_cd
			,fiscal_dt
			,event_gmt_dt
			,event_gmt_ts
			,event_rptg_gmt_ts
			,delivery_sched_local_dt
			,all_tracking_nr
			,uplift_reqd_ind
			,reason_cd			
	    ,reason_desc       
	    ,scac_first_occurrence_ind			
	    ,scac_last_occurrence_ind		
		)
		SELECT 
			wrk3.event_handler_nr 
			,wrk3.shipment_id
			,wrk3.region_cd
			,wrk3.sender_nr 
			,wrk3.sender_name
			,wrk3.event_local_dt
			,wrk3.b2b_received_ts
			,wrk3.b2b_received_dt
			,wrk3.loc_1_desc
			,wrk3.event_msg_nr
			,wrk3.ship_to_country_cd
			,wrk3.event_cd	 
			,wrk3.event_phase_cd
			,wrk3.event_name
			,
			CASE WHEN wrk3.event_gmt_dt > '2000-01-01' AND wrk3.event_gmt_dt < '2030-01-01' 
			AND wrk3.b2b_received_dt > '2000-01-01' AND wrk3.b2b_received_dt < '2030-01-01'	 THEN	
			(
			(EXTRACT(DAY FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))*24.00) +
			(EXTRACT(HOUR FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))) +
			(EXTRACT(MINUTE FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))/60.00) + 
			(EXTRACT(SECOND FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))/3600.00)
			) ELSE NULL END AS diff_in_hour_val
			,
			CASE WHEN wrk3.event_gmt_dt > '2000-01-01' AND wrk3.event_gmt_dt < '2030-01-01' 
			AND wrk3.b2b_received_dt > '2000-01-01' AND wrk3.b2b_received_dt < '2030-01-01'	 THEN	
			(
			(EXTRACT(DAY FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))*24.00) +
			(EXTRACT(HOUR FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))) +
			(EXTRACT(MINUTE FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))/60.00) + 
			(EXTRACT(SECOND FROM (wrk3.b2b_received_ts - wrk3.event_gmt_ts DAY(4) TO SECOND(0)))/3600.00)
			) ELSE NULL END AS Diff_In_Sec_Val
			,wrk3.orig_scac_cd
			,wrk3.parent_scac_cd
			,wrk5.fiscal_dt
			,wrk3.event_gmt_dt
			,wrk3.event_gmt_ts
			,wrk3.event_rptg_gmt_ts
			,wrk3.delivery_sched_local_dt
			,wrk3.all_tracking_nr
			,wrk3.uplift_reqd_ind
			,wrk3.reason_cd			
	    ,wrk3.reason_desc		  
	    ,wrk3.scac_first_occurrence_ind			
	    ,wrk3.scac_last_occurrence_ind		
		FROM 
			logistics_app.wrk_em_carrier_dq_event_dtls wrk3 
			LEFT OUTER JOIN logistics_app.wrk_em_dq_fiscal_dates  wrk5
				ON (wrk3.shipment_id = wrk5.shipment_id AND wrk3.event_handler_nr=wrk5.event_handler_nr AND wrk3.parent_scac_cd = wrk5.parent_scac_cd AND wrk3.sender_nr=wrk5.sender_nr)
		WHERE	
			(
				(
				wrk3.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE occurrence_ind in ('F','B')) AND wrk3.scac_first_occurrence_ind=1 
				)
				OR (
				wrk3.event_cd IN (SELECT event_cd FROM logistics.em_carrier_dq_event_list WHERE occurrence_ind='L') AND wrk3.scac_last_occurrence_ind=1 
				)
			)
		AND wrk3.region_cd IN ('EURO')
		;


		COLLECT STATS ON  logistics_app.wrk_em_dq_event_latency_dtls ;







		SET  lv_msg = 'Inserting into work table wrk_em_dq_event_uplift_dtls' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		INSERT INTO logistics_app.wrk_em_dq_event_uplift_dtls 
		(
			event_handler_nr,
			shipment_id,
			region_cd,
			sender_nr  ,
			sender_name,
			event_local_dt,
			b2b_received_ts,
			b2b_received_dt,
			loc_1_desc ,
			event_msg_nr,
			event_cd,
			event_phase_cd ,
			event_name ,
			diff_in_hour_val,
			diff_in_sec_val,
			orig_scac_cd,
			parent_scac_cd,
			fiscal_dt,
			event_gmt_dt,
			event_gmt_ts,
			event_rptg_gmt_ts,
			delivery_sched_local_dt,
			inbound_mit_ind ,
			display_process_type_cd ,
			pod_state_cd ,
			pod_country_cd,
			pod_zip_cd ,
			ship_from_country_cd,
			uplift_reqd_ind,
			shipment_mode_cd,
			shipping_point_cd,
			ship_point_desc,
			completion_weekly_sent_ind,
			completion_monthly_sent_ind,
			completion_quarterly_sent_ind,
			latency_weekly_sent_ind,
			latency_monthly_sent_ind,
			latency_quarterly_sent_ind,
			all_tracking_nr ,
			ship_from_city_name,
			chargeable_wt_kg_msr,	      
			chargeable_wt_lb_msr,	      
			reason_cd,		      
	    reason_desc,		  
	    scac_first_occurrence_ind,
	    scac_last_occurrence_ind,
	    smmry_rptg_cd,
	    scheduled_delivery_gmt_ts,
	    carrier_leg_cd,
	    service_cd
		)
		SELECT 
			wrk6.event_handler_nr ,
			wrk6.shipment_id,
			wrk6.region_cd ,
			wrk6.sender_nr ,
			wrk6.sender_name,
			wrk6.event_local_dt,
			wrk6.b2b_received_ts ,
			wrk6.b2b_received_dt,
			REGEXP_REPLACE(wrk6.loc_1_desc,',',' ',1,0,'i') ,
			wrk6.event_msg_nr,
			wrk6.event_cd ,
			wrk6.event_phase_cd ,
			wrk6.event_name,
			wrk6.diff_in_hour_val,
			wrk6.diff_in_sec_val,
			CASE WHEN wrk6.orig_scac_cd='TNTD' AND ehs.ship_condition_cd IN ('A1','A3') THEN 'TNTD-Bulk' WHEN wrk6.orig_scac_cd='TNTD' THEN 'TNTD-Parcel' ELSE wrk6.orig_scac_cd END,
			CASE WHEN wrk6.parent_scac_cd='TNTD' AND ehs.ship_condition_cd IN ('A1','A3') THEN 'TNTD-Bulk' WHEN wrk6.parent_scac_cd='TNTD' THEN 'TNTD-Parcel' ELSE wrk6.parent_scac_cd END,
			wrk6.fiscal_dt,
			wrk6.event_gmt_dt,
			wrk6.event_gmt_ts,
			wrk6.event_rptg_gmt_ts,
			wrk6.delivery_sched_local_dt,
			CASE WHEN (CASE WHEN ehs.process_type_cd IN ('ZIPO','ZI3P','ZIPS') THEN ehs.ship_from_nr  ELSE ehs.shipping_point_cd END) LIKE 'MIT%' THEN 'Y' ELSE 'N' END  AS inbound_mit_flag,
			ehs.display_process_type_cd,
			COALESCE(ehs.ship_to_state_cd, ehs.ship_to_district_name)   AS	pod_state_cd  ,		    
			wrk6.ship_to_country_cd									AS	 pod_country_cd	 ,	      
			ehs.ship_to_zip_cd										AS	pod_zip_cd ,
			ehs.ship_from_country_cd,
			CASE	
		WHEN ehs.region_cd ='PAC' AND ehs.process_type_cd IN  ('ZIPS') THEN 'N' 
		WHEN ehs.region_cd ='AMR' AND ehs.ship_from_country_cd = 'US' AND wrk6.Ship_To_Country_Cd IN ('MX','CA','US','CO') THEN 'N' 
		WHEN ehs.ship_from_country_cd = 'CN' AND UPPER(ehs.ship_from_city_name) LIKE '%SHENZHEN%' AND wrk6.Ship_To_Country_Cd IN ('HK','MO') THEN 'N' 
		WHEN ehs.region_cd='EURO' AND ehs.ship_from_country_cd NOT IN ('CN','KR','JP','US') and ehs.ship_from_country_cd <> wrk6.ship_to_country_cd  THEN 'N'
		WHEN ehs.region_cd='PAC' AND ehs.ship_from_country_cd NOT IN ('CN','US') AND ehs.ship_from_country_cd <> wrk6.ship_to_country_cd THEN 'N'
		WHEN ehs.ship_from_country_cd <> wrk6.ship_to_country_cd THEN 'Y'
		ELSE 'N' END AS uplift_reqd_ind ,
			CASE	
				WHEN ehs.transportation_mode_cd IN ('A1','A5','0002') or ehs.transportation_mode_cd IS NULL THEN 'Air'
				WHEN ehs.transportation_mode_cd IN ('OCEAN','0003') THEN 'Ocean'
				WHEN ehs.transportation_mode_cd IN ('A3','A7','0001','A6','A4','A8') THEN 'Ground' END AS shipment_mode_cd,
			CASE WHEN ehs.process_type_cd IN ('ZIPO','ZI3P','ZIPS') THEN ehs.ship_from_nr  ELSE ehs.shipping_point_cd END AS shipping_point_cd,
			CASE WHEN ehs.process_type_cd IN ('ZIPO','ZIPS','ZI3P') THEN ehs.ship_from_name1_desc ELSE esp.ship_point_desc END AS ship_point_desc,
			'Y' AS completion_weekly_sent_ind,
			'Y' AS completion_monthly_sent_ind,
			'Y' AS completion_quarterly_sent_ind,
			'Y' AS latency_weekly_sent_ind,
			'Y' AS latency_monthly_sent_ind,
			'Y' AS latency_quarterly_sent_ind, 
			REGEXP_REPLACE(wrk6.all_tracking_nr,',',' ',1,0,'i'), 
			UPPER(ehs.ship_from_city_name),
			CASE WHEN ehs.chargeable_wt_uom_cd='LB' THEN (0.453592*Chargeable_Wt_Msr) ELSE chargeable_wt_msr END AS chargeable_wt_kg_msr,	 
	    CASE WHEN ehs.chargeable_wt_uom_cd='KG' THEN (2.20462*Chargeable_Wt_Msr) ELSE chargeable_wt_msr END AS chargeable_wt_lb_msr, 
			wrk6.reason_cd,					       
			wrk6.reason_desc ,
			wrk6.scac_first_occurrence_ind ,
			wrk6.scac_last_occurrence_ind  ,
			CASE 
				WHEN wrk6.event_phase_cd='Estimated' AND wrk6.event_name='Delivered' AND wrk6.scac_first_occurrence_ind=1 AND wrk6.scac_last_occurrence_ind=1 THEN 'Y' 
				WHEN wrk6.event_phase_cd='Estimated' AND wrk6.event_name='Delivered' AND wrk6.scac_first_occurrence_ind=1 AND wrk6.scac_last_occurrence_ind=0 THEN 'N' 
				ELSE 'Y' END AS smmry_rptg_cd, 
			CASE WHEN SUBSTR(ehs.scheduled_delivery_timezone_cd,1,1)='-' THEN ehs.scheduled_delivery_ts 
			 + CAST (  SUBSTR(ehs.scheduled_delivery_timezone_cd,2,2) AS INTERVAL  HOUR) 
			 + CAST (  SUBSTR(ehs.scheduled_delivery_timezone_cd,5,2) AS INTERVAL  MINUTE)
			 WHEN SUBSTR(ehs.scheduled_delivery_timezone_cd,1,1)='+' THEN ehs.scheduled_delivery_ts 
			 - CAST (  SUBSTR(ehs.Scheduled_Delivery_Timezone_Cd,2,2) AS INTERVAL  HOUR) 
			 - CAST (  SUBSTR(ehs.Scheduled_Delivery_Timezone_Cd,5,2) AS INTERVAL  MINUTE)
			ELSE ehs.scheduled_delivery_ts END AS scheduled_delivery_gmt_ts,
			'1st' AS carrier_leg_cd,
			'D-T-D' AS service_cd	    
		FROM 
			logistics_app.wrk_em_dq_event_latency_dtls wrk6 
			INNER JOIN supply_chain.evt_handler_shipment_cur ehs 
				ON ( wrk6.shipment_id = ehs.shipment_id AND wrk6.event_handler_nr = ehs.event_handler_nr ) 
			LEFT OUTER JOIN logistics.em_ship_point esp 
				ON ( wrk6.region_cd=esp.region_cd AND ehs.shipping_point_cd=esp.ship_point_cd)
			WHERE wrk6.parent_scac_cd IS NOT NULL
		;


		SET  lv_msg = 'Delete from work table wrk_em_dq_event_uplift_dtls for RMA' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
		
		DELETE FROM logistics_app.wrk_em_dq_event_uplift_dtls WHERE display_process_type_cd IN ('RMA');
		
		SET  lv_msg = 'Updating work table wrk_em_carrier_dq_event_dtls for marking uplift Y for PAC' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
		
		UPDATE a FROM
			logistics_app.wrk_em_dq_event_uplift_dtls a , 
			(SELECT x.shipment_id, x.event_handler_nr FROM logistics_app.wrk_em_dq_uplift_ind x GROUP BY 1,2) b 
		SET uplift_reqd_ind = 'N' 
			,carrier_leg_cd='2nd'
			,service_cd='A-T-D'
		WHERE 
			a.shipment_id = b.shipment_id AND 
			a.event_handler_nr = b.event_handler_nr 
			
			
		; 
			
		UPDATE a FROM
			logistics_app.wrk_em_dq_event_uplift_dtls a , 
			(SELECT shipment_id, event_handler_nr, parent_scac_cd FROM logistics_app.wrk_em_dq_uplift_ind GROUP BY 1,2,3) b 
		SET uplift_reqd_ind = 'Y' 
			,carrier_leg_cd='1st'
			,service_cd='D-T-A'
		WHERE 
			a.shipment_id = b.shipment_id AND 
			a.event_handler_nr = b.event_handler_nr AND
			a.parent_scac_cd = b.parent_scac_cd AND
			a.region_cd IN ( 'PAC','AMR')	
		; 
		
		
		
		
		SET  lv_msg = 'Updating work table wrk_em_carrier_dq_event_dtls for marking uplift Y for EURO' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));

		UPDATE a FROM
			logistics_app.wrk_em_dq_event_uplift_dtls a , 
			(SELECT shipment_id, event_handler_nr, parent_scac_cd,sender_nr FROM logistics_app.wrk_em_dq_uplift_ind GROUP BY 1,2,3,4) b 
		SET uplift_reqd_ind = 'Y' 
			,carrier_leg_cd='1st'
		WHERE 
			a.shipment_id = b.shipment_id AND 
			a.event_handler_nr = b.event_handler_nr AND
			a.parent_scac_cd = b.parent_scac_cd AND
			a.sender_nr = b.sender_nr AND
			a.region_cd = 'EURO' 
		; 
		
		

		COLLECT STATS ON  logistics_app.wrk_em_dq_event_uplift_dtls;



		UPDATE logistics_app.em_carrier_dq_raw_file
		SET compltn_wkly_sent_ind='Y' 
		WHERE compltn_wkly_sent_ind='N' ;

		UPDATE logistics_app.em_carrier_dq_raw_file
		SET latency_wkly_sent_ind ='Y' 
		WHERE latency_wkly_sent_ind ='N' ;



		IF ln_monthly_wk = 1 THEN 

			UPDATE logistics_app.em_carrier_dq_raw_file
			SET compltn_mthly_sent_ind='Y' 
			WHERE compltn_mthly_sent_ind='N' ;

			UPDATE logistics_app.em_carrier_dq_raw_file
			SET latency_mthly_sent_ind='Y' 
			WHERE latency_mthly_sent_ind='N' ;

		END IF ;



		IF ln_quarterly_wk  = 1 THEN 

			UPDATE logistics_app.em_carrier_dq_raw_file
			SET compltn_qtrly_sent_ind ='Y' 
			WHERE compltn_qtrly_sent_ind='N';

			UPDATE logistics_app.em_carrier_dq_raw_file
			SET latency_qtrly_sent_ind  ='Y' 
			WHERE latency_qtrly_sent_ind ='N';

		END IF ;






		SET  lv_msg = 'Deleteing main table EM_CARRIER_DQ_RAW_FILE for PAC and AMR' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
		
		DELETE 
		FROM 
			logistics_app.em_carrier_dq_raw_file ecd ,
			logistics_app.wrk_em_dq_event_uplift_dtls wrk7 
		WHERE 
			ecd.shipment_id=wrk7.shipment_id 
			AND ecd.event_handler_nr=wrk7.event_handler_nr
			AND ecd.event_phase_cd=wrk7.event_phase_cd
			AND ecd.event_name=wrk7.event_name
			AND ecd.parent_scac_cd=wrk7.parent_scac_cd
			AND ecd.region_cd IN ('PAC','AMR')
			AND ecd.smmry_rptg_cd=wrk7.smmry_rptg_cd		
		; 
		
		SET  lv_msg = 'Deleteing main table EM_CARRIER_DQ_RAW_FILE for EURO' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
		
		DELETE 
		FROM 
			logistics_app.em_carrier_dq_raw_file ecd ,
			logistics_app.wrk_em_dq_event_uplift_dtls wrk7 
		WHERE 
			ecd.shipment_id=wrk7.shipment_id 
			AND ecd.event_handler_nr=wrk7.event_handler_nr
			AND ecd.event_phase_cd=wrk7.event_phase_cd
			AND ecd.event_name=wrk7.event_name
			AND ecd.parent_scac_cd=wrk7.parent_scac_cd
			AND ecd.sender_nr=wrk7.sender_nr
			AND ecd.region_cd IN ('EURO')
			AND ecd.smmry_rptg_cd=wrk7.smmry_rptg_cd		
		;
		
		SET  lv_msg = 'Inserting into main table EM_CARRIER_DQ_RAW_FILE' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));



		INSERT INTO logistics_app.em_carrier_dq_raw_file 
		(
			event_handler_nr
			,shipment_id
			,region_cd
			,inbound_mit_ind
			,sales_order_nr
			,delivery_nr
			,po_nr
			,doc_type_cd
			,display_process_type_cd
			,all_tracking_nr
			,orig_scac_cd
			,sender_nr 
			,sender_name
			,ship_point_cd
			,ship_point_desc
			,event_dt
			,b2b_received_ts
			,b2b_dt
			,diff_in_hour_val
			,loc_desc 
			,pod_state_cd
			,pod_country_cd
			,ship_from_country_cd
			,ship_from_city_name
			,pod_zip_cd
			,current_event_name
			,current_event_ts
			,incomplete_delivery_rsn_2_cd
			,diff_in_sec_val
			,event_msg_nr
			,fiscal_dt
			,event_gmt_dt
			,event_gmt_ts
			,event_rptg_gmt_ts
			,pod_event_ind
			,delivery_sched_local_dt
			,event_cd
			,event_phase_cd
			,event_name
			,shipment_mode_cd
			,uplift_reqd_ind
			,carrier_name
			,parent_scac_cd
			,parent_carrier_name
			,carrier_type_cd
			,compltn_wkly_sent_ind
			,compltn_mthly_sent_ind
			,compltn_qtrly_sent_ind
			,latency_wkly_sent_ind
			,latency_mthly_sent_ind
			,latency_qtrly_sent_ind
			,chargeable_wt_kg_msr	       
	    	,chargeable_wt_lb_msr	   
	    	,reason_cd			   
	    	,reason_desc		   
	    	,scac_first_occurrence_ind	   
	    	,scac_last_occurrence_ind		
	    	,smmry_rptg_cd					
	    	,scheduled_delivery_gmt_ts	    
	    	,carrier_leg_cd
	    	,event_phase_name
	    	,service_cd
			,ldw_batch_sk					
			,ldw_change_ts					
			
			
			
		)
		SELECT
			wrk7.event_handler_nr
			,wrk7.shipment_id	
			,wrk7.region_cd
			,wrk7.inbound_mit_ind
			,wrk2.sales_order_nr
			,wrk2.delivery_nr
			,wrk2.po_nr
			,wrk2.doc_type_cd
			,wrk7.display_process_type_cd
			,REGEXP_REPLACE(wrk7.all_tracking_nr,'T0(\S+)\s?','',1,0,'i') 
			,wrk7.orig_scac_cd
			,wrk7.sender_nr 
			,wrk7.sender_name
			,wrk7.shipping_point_cd
			,REGEXP_REPLACE(wrk7.ship_point_desc,',',' ',1,0,'i')
			,wrk7.event_local_dt
			,wrk7.b2b_received_ts
			,wrk7.b2b_received_dt
			,wrk7.diff_in_hour_val
			,wrk7.loc_1_desc
			,wrk7.pod_state_cd
			,wrk7.pod_country_cd
			,wrk7.ship_from_country_cd
			,wrk7.ship_from_city_name
			,wrk7.pod_zip_cd
			,sme.current_event_name
			,sme.current_event_ts
			,sme.incomplete_delivery_rsn_2_cd
			,wrk7.diff_in_sec_val
			,wrk7.event_msg_nr
			,wrk7.fiscal_dt
			,wrk7.event_gmt_dt
			,wrk7.event_gmt_ts
			,wrk7.event_rptg_gmt_ts
			,wrk7.pod_event_ind
			,wrk7.delivery_sched_local_dt
			,wrk7.event_cd
			,wrk7.event_phase_cd
			,wrk7.event_name
			,wrk7.shipment_mode_cd
			,wrk7.uplift_reqd_ind
			,sca.carrier_name
			,wrk7.parent_scac_cd
			,sca.parent_carrier_name
			,sca.carrier_type_cd
			,wrk7.completion_weekly_sent_ind
			,wrk7.completion_monthly_sent_ind
			,wrk7.completion_quarterly_sent_ind
			,wrk7.latency_weekly_sent_ind
			,wrk7.latency_monthly_sent_ind
			,wrk7.latency_quarterly_sent_ind
			,wrk7.chargeable_wt_kg_msr
	    ,wrk7.chargeable_wt_lb_msr
	    ,wrk7.reason_cd  
	    ,wrk7.reason_desc 
	    ,wrk7.scac_first_occurrence_ind	   
	    ,wrk7.scac_last_occurrence_ind		
	    ,wrk7.smmry_rptg_cd					
	    ,wrk7.scheduled_delivery_gmt_ts	
	    ,wrk7.carrier_leg_cd
	    ,wrk7.service_cd
	    ,CASE WHEN wrk7.event_phase_cd='Estimated' THEN wrk7.event_phase_cd||' '||wrk7.event_name ELSE wrk7.event_name END AS event_phase_name
			,in_batch_sk
			,CURRENT_TIMESTAMP(0)
		FROM 
			logistics_app.wrk_em_dq_event_uplift_dtls wrk7
			LEFT OUTER JOIN supply_chain.shipment_multi_event_cur Sme 
				ON ( sme.shipment_id = wrk7.shipment_id AND sme.event_handler_nr = wrk7.event_handler_nr)
			LEFT OUTER JOIN logistics_app.wrk_em_dq_doc_id_info wrk2 
				ON ( wrk2.shipment_id= wrk7.shipment_id AND wrk2.event_handler_nr = wrk7.event_handler_nr)
			LEFT OUTER JOIN logistics.em_scac_map_ref sca ON (sca.child_scac_cd= wrk7.orig_scac_cd AND sca.em_region_cd  = wrk7.region_cd )
		;

		
	
		DELETE FROM logistics_app.wrk_em_dq_cancelled_shipments;
			
		INSERT INTO logistics_app.wrk_em_dq_cancelled_shipments(
		shipment_id
		,event_handler_nr
		)
		SELECT 
		shipment_id 
		,event_handler_nr 
		FROM logistics_app.em_carrier_dq_raw_file b 
		WHERE event_cd IN ('A3','CA') GROUP BY 1,2
		;
		
		DELETE a FROM logistics_app.em_carrier_dq_raw_file a , logistics_app.wrk_em_dq_cancelled_shipments b 
		WHERE a.shipment_id=b.shipment_id AND a.event_handler_nr=b.event_handler_nr AND a.region_cd IN ('AMR','EURO'); 



		UPDATE logistics_app.em_carrier_dq_raw_file
		SET compltn_wkly_sent_ind='N' 
		WHERE fiscal_dt BETWEEN ld_start_dt AND ld_end_dt 
		AND smmry_rptg_cd='Y' AND event_cd NOT IN ('AP','SD');			

		UPDATE logistics_app.em_carrier_dq_raw_file
		SET latency_wkly_sent_ind='N' 
		WHERE B2B_Dt BETWEEN ld_start_dt AND ld_end_dt
		AND smmry_rptg_cd='Y';			
		
		
		
		UPDATE logistics_app.em_carrier_dq_raw_file
		SET compltn_wkly_sent_ind='Y'											
		WHERE TRIM(ship_from_country_cd)||TRIM(pod_country_cd) NOT IN ('CNUS','CNCA','CNMX','CNBR','CNCO','CNCL','USBR','USCO','USCL','TWUS','TWCA','TWMX','TWBR','TWCO','TWCL')
		AND event_cd IN ('B6','X4','X5','X3','P1')
		AND compltn_wkly_sent_ind='N' 
		AND region_cd IN ('AMR');
		
		UPDATE logistics_app.em_carrier_dq_raw_file
		SET compltn_wkly_sent_ind='Y'
		WHERE TRIM(ship_from_country_cd)||TRIM(pod_country_cd) NOT IN ('CNUS','CNCA','CNMX','CNCO','CNCL','USCO','USCL','TWUS','TWCA','TWMX','TWCO','TWCL')
		AND event_cd IN ('K1','X6')
		AND compltn_wkly_sent_ind='N' 
		AND region_cd IN ('AMR');
		
		UPDATE logistics_app.em_carrier_dq_raw_file
		SET compltn_wkly_sent_ind='Y'
		WHERE ship_from_city_name LIKE '%SHENZHEN%'
		AND TRIM(pod_country_cd)= 'HK'
		AND event_cd IN ('B6','X4','X5','X3','P1','K1','X6')
		AND compltn_wkly_sent_ind='N' 
		AND region_cd IN ('PAC');
		
		
		
		
		UPDATE a FROM logistics_app.em_carrier_dq_raw_file a, logistics.em_carrier_dq_exclusion b
		SET compltn_wkly_sent_ind='Y'
		WHERE 
			a.region_cd=b.region_cd
		AND a.ship_from_country_cd=b.ship_from_country_cd
		AND a.pod_country_cd=b.ship_to_country_cd
		AND a.display_process_type_cd=b.process_type_cd
		AND a.parent_scac_cd=b.scac_cd 
		AND a.carrier_leg_cd=b.carrier_leg_cd
		AND a.event_phase_name=b.event_name 
		AND b.exclude_cd='XO'
		AND a.shipment_mode_cd='Ocean'
		AND a.compltn_wkly_sent_ind='N';
		
		
		
		UPDATE a FROM logistics_app.em_carrier_dq_raw_file a, logistics.em_carrier_dq_exclusion b
		SET compltn_wkly_sent_ind='Y'
		WHERE 
			a.region_cd=b.region_cd
		AND a.ship_from_country_cd=b.ship_from_country_cd
		AND a.pod_country_cd=b.ship_to_country_cd
		AND a.display_process_type_cd=b.process_type_cd
		AND a.parent_scac_cd=b.scac_cd 
		AND a.carrier_leg_cd=b.carrier_leg_cd
		AND a.event_phase_name=b.event_name 
		AND b.exclude_cd='X'
		AND a.compltn_wkly_sent_ind='N';
		




		SET  lv_msg = 'Inserting into main table wrk_em_dq_sent_items for PAC and AMR' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
		
		DELETE FROM logistics_app.wrk_em_dq_sent_items;
		
		INSERT INTO logistics_app.wrk_em_dq_sent_items
		(
			shipment_id ,
			event_handler_nr,
			parent_scac_cd
		)
		SELECT
			shipment_id,
			event_handler_nr,
			parent_scac_cd
		FROM 
			logistics.em_carrier_dq_raw_file
		WHERE 
				(compltn_wkly_sent_ind='N' 
				OR 
				latency_wkly_sent_ind='N'
				)
			AND region_cd IN ('PAC','AMR')
		GROUP BY  1,2,3
		;

		SET  lv_msg = 'Inserting into main table wrk_em_dq_sent_items for EURO' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
				
		INSERT INTO logistics_app.wrk_em_dq_sent_items
		(
			shipment_id ,
			event_handler_nr,
			parent_scac_cd,
			sender_nr
		)
		SELECT
			shipment_id,
			event_handler_nr,
			parent_scac_cd,
			sender_nr
		FROM 
			logistics.em_carrier_dq_raw_file
		WHERE 
			(	compltn_wkly_sent_ind='N' 
				OR 
				latency_wkly_sent_ind='N'
			)
			AND region_cd IN ('EURO')
		GROUP BY  1,2,3,4
		;

		COLLECT STATS ON  logistics_app.wrk_em_dq_sent_items;

		





		DELETE FROM logistics_app.EM_CARRIER_DQ_WK_ACCURACY;
			
		INSERT INTO logistics_app.EM_CARRIER_DQ_WK_ACCURACY
		(
		 shipment_id		 
		,event_handler_nr	 
		,parent_scac_cd		 
		,sender_nr     
		,sender_name	      
		,fiscal_dt		 
		,region_cd
		,scheduled_delivery_gmt_ts
		,ship_from_country_cd
		,ship_to_country_cd
		,carrier_chg_wt_ind
		,associated_reason_ind			
		,appt_reg_gmt_dt	 
		,appt_reg_gmt_ts	 
		,appt_reg_b2b_dt
		,appt_reg_b2b_ts       
		,fst_est_del_gmt_dt	 
		,fst_est_del_gmt_ts	 
		,fst_est_del_b2b_dt
		,fst_est_del_b2b_ts
		,lst_est_del_gmt_dt
		,lst_est_del_gmt_ts	 
		,lst_est_del_b2b_dt
		,lst_est_del_b2b_ts  
		,del_gmt_dt		 
		,del_gmt_ts		 
		,del_b2b_dt
		,del_b2b_ts	     
		,del_appt_gmt_dt	 
		,del_appt_gmt_ts	 
		,del_appt_b2b_dt
		,del_appt_b2b_ts       
		,fnl_arpt_dest_gmt_dt	 
		,fnl_arpt_dest_gmt_ts	 
		,fnl_arpt_dest_b2b_dt
		,fnl_arpt_dest_b2b_ts
		,shp_pkup_gmt_dt	 
		,shp_pkup_gmt_ts	 
		,shp_pkup_b2b_dt
		,shp_pkup_b2b_ts   
		,report_type_ind
		) 
	SELECT
		ecdr.shipment_id	     
		,ecdr.event_handler_nr	      
		,ecdr.parent_scac_cd	      
		,MAX(ecdr.sender_nr)   
		,MAX(ecdr.sender_name)		  
		,fiscal_dt		 
		,region_cd
		,scheduled_delivery_gmt_ts
		,ship_from_country_cd
		,pod_country_cd
		,MAX(CASE WHEN	chargeable_wt_kg_msr IS NULL AND chargeable_wt_lb_msr IS NULL THEN '0' ELSE '1' END) AS carrier_chg_wt_ind
		,MAX(CASE WHEN event_name IN ('Shipment Delay','Delivery Not Completed') AND reason_cd <>'NS' THEN '1' ELSE '0' END)  AS associated_reason_ind			
		,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_dt END)
		,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_ts END)  
		,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_dt END)
		,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_received_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_dt END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_dt END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_received_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_dt END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_dt END) 
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_received_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_dt	 END)  
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_dt END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_received_ts END)
		,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_dt END)
		,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_ts END )  
		,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_dt END)
		,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_received_ts END)
		,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_dt END)
		,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_ts END)
		,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_dt END)
		,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_received_ts END)
		,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_dt END)
		,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_ts END)
		,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_dt END)
		,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_received_ts END)
		,MIN(CASE WHEN compltn_wkly_sent_ind='N' AND latency_wkly_sent_ind='N' THEN 'B'
			WHEN compltn_wkly_sent_ind='N' AND latency_wkly_sent_ind='Y' THEN 'C'
			WHEN compltn_wkly_sent_ind='Y' AND latency_wkly_sent_ind='N' THEN 'L'
			WHEN compltn_wkly_sent_ind='X' AND latency_wkly_sent_ind='N' THEN 'L'
			ELSE NULL END) AS "Report_type_ind"
   FROM logistics.em_carrier_dq_raw_file ecdr
   INNER JOIN logistics_app.wrk_em_dq_sent_items wed 
				ON (ecdr.shipment_id=wed.shipment_id AND ecdr.event_handler_nr=wed.event_handler_nr AND ecdr.parent_scac_cd=wed.parent_scac_cd )  
   WHERE region_cd IN ('PAC','AMR') 
   GROUP BY 1,2,3,6,7,8,9,10
   ;	 


INSERT INTO logistics_app.EM_CARRIER_DQ_WK_ACCURACY
		(
		 shipment_id		 
		,event_handler_nr	 
		,parent_scac_cd		 
		,sender_nr
		,sender_name		   
		,fiscal_dt		 
		,region_cd
		,scheduled_delivery_gmt_ts
		,ship_from_country_cd
		,ship_to_country_cd
		,carrier_chg_wt_ind
		,associated_reason_ind					  
		,appt_reg_gmt_dt	 
		,appt_reg_gmt_ts	 
		,appt_reg_b2b_dt
		,appt_reg_b2b_ts       
		,fst_est_del_gmt_dt	 
		,fst_est_del_gmt_ts	 
		,fst_est_del_b2b_dt
		,fst_est_del_b2b_ts
		,lst_est_del_gmt_dt
		,lst_est_del_gmt_ts	 
		,lst_est_del_b2b_dt
		,lst_est_del_b2b_ts  
		,del_gmt_dt		 
		,del_gmt_ts		 
		,del_b2b_dt
		,del_b2b_ts	     
		,del_appt_gmt_dt	 
		,del_appt_gmt_ts	 
		,del_appt_b2b_dt
		,del_appt_b2b_ts       
		,fnl_arpt_dest_gmt_dt	 
		,fnl_arpt_dest_gmt_ts	 
		,fnl_arpt_dest_b2b_dt
		,fnl_arpt_dest_b2b_ts
		,shp_pkup_gmt_dt	 
		,shp_pkup_gmt_ts	 
		,shp_pkup_b2b_dt
		,shp_pkup_b2b_ts   
		,report_type_ind
		) 
	SELECT
		ecdr.shipment_id	     
		,ecdr.event_handler_nr	      
		,ecdr.parent_scac_cd	      
		,ecdr.sender_nr	   
		,ecdr.sender_name	    
		,fiscal_dt		 
		,region_cd
		,scheduled_delivery_gmt_ts
		,ship_from_country_cd
		,pod_country_cd
		,MAX(CASE WHEN chargeable_wt_kg_msr IS NULL AND chargeable_wt_lb_msr IS NULL THEN '0' ELSE '1' END) AS carrier_chg_wt_ind
		,MAX(CASE WHEN event_name IN ('Shipment Delay','Delivery Not Completed') AND reason_cd <>'NS' THEN '1' ELSE '0' END)  AS associated_reason_ind			
		,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_dt END)
		,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_ts END)  
		,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_dt END)
		,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_received_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_dt END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_dt END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_received_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_dt END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_dt END) 
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_received_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_dt	 END)  
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_ts END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_dt END)
		,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_received_ts END)
		,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_dt END)
		,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_ts END )  
		,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_dt END)
		,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_received_ts END)
		,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_dt END)
		,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_ts END)
		,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_dt END)
		,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_received_ts END)
		,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_dt END)
		,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_ts END)
		,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_dt END)
		,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_received_ts END)		
		,MIN(CASE WHEN compltn_wkly_sent_ind='N' AND latency_wkly_sent_ind='N' THEN 'B'
			WHEN compltn_wkly_sent_ind='N' AND latency_wkly_sent_ind='Y' THEN 'C'
			WHEN compltn_wkly_sent_ind='Y' AND latency_wkly_sent_ind='N' THEN 'L'
			WHEN compltn_wkly_sent_ind='X' AND latency_wkly_sent_ind='N' THEN 'L'
			ELSE NULL END) AS "Report_type_ind"
   FROM logistics.em_carrier_dq_raw_file ecdr
   INNER JOIN logistics_app.wrk_em_dq_sent_items wed 
			ON (ecdr.shipment_id=wed.shipment_id AND ecdr.event_handler_nr=wed.event_handler_nr AND ecdr.sender_nr = wed.sender_nr AND ecdr.parent_scac_cd=wed.parent_scac_cd )  
   WHERE region_cd IN ('EURO') 
   GROUP BY 1,2,3,4,5,6,7,8,9,10
   ;	 



	UPDATE logistics_app.em_carrier_dq_wk_accuracy 
    SET
		    del_appt_diff_hr_val      = CASE WHEN del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (del_appt_gmt_ts - del_gmt_ts	DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (del_appt_gmt_ts - del_gmt_ts	 DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (del_appt_gmt_ts - del_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (del_appt_gmt_ts - del_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END
      
		    ,del_appt_diff_dy_val     = CASE WHEN del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									del_appt_gmt_dt -  del_gmt_dt
									ELSE NULL END
	
		    ,appt_req_diff_hr_val     = CASE WHEN appt_reg_gmt_dt > '2000-01-01' AND appt_reg_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
										EXTRACT(DAY FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END
	
		    ,appt_req_diff_dy_val     = CASE WHEN appt_reg_gmt_dt > '2000-01-01' AND appt_reg_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
										appt_reg_gmt_dt - CAST(scheduled_delivery_gmt_ts AS DATE)
										ELSE NULL END

		    ,est_sdd_diff_hr_val      = CASE WHEN fst_est_del_gmt_dt > '2000-01-01' AND fst_est_del_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN
										EXTRACT(DAY FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END
	
		    ,est_sdd_diff_dy_val      = CASE WHEN fst_est_del_gmt_dt > '2000-01-01' AND fst_est_del_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN
										fst_est_del_gmt_dt - CAST(scheduled_delivery_gmt_ts AS DATE)
										ELSE NULL END
	
		    ,est_del_appt_diff_hr_val = CASE WHEN lst_est_del_gmt_dt > '2000-01-01' AND lst_est_del_gmt_dt < '2030-01-01' 
										AND del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (lst_est_del_gmt_ts - del_appt_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (lst_est_del_gmt_ts - del_appt_gmt_ts	 DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (lst_est_del_gmt_ts - del_appt_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (lst_est_del_gmt_ts - del_appt_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END

		    ,est_del_appt_diff_dy_val = CASE WHEN lst_est_del_gmt_dt > '2000-01-01' AND lst_est_del_gmt_dt < '2030-01-01' 
										AND del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01'  THEN   
									lst_est_del_gmt_dt - del_appt_gmt_dt
									ELSE NULL END

		    ,est_pod_diff_hr_val      = CASE WHEN lst_est_del_b2b_dt > '2000-01-01' AND lst_est_del_b2b_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									lst_est_del_b2b_dt - del_gmt_dt
									ELSE NULL END						

		    ,est_pod_diff_dy_val      = CASE WHEN lst_est_del_gmt_dt > '2000-01-01' AND lst_est_del_gmt_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									lst_est_del_gmt_dt - del_gmt_dt 
									ELSE NULL END						

		    ,est_pkup_diff_hr_val     = CASE WHEN fst_est_del_b2b_dt > '2000-01-01' AND fst_est_del_b2b_dt < '2030-01-01' 
										AND shp_pkup_gmt_dt > '2000-01-01' AND shp_pkup_gmt_dt < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts	 DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END

		    ,est_pkup_diff_dy_val     = CASE WHEN fst_est_del_gmt_dt > '2000-01-01' AND fst_est_del_gmt_dt < '2030-01-01' 
										AND shp_pkup_gmt_dt > '2000-01-01' AND shp_pkup_gmt_dt < '2030-01-01'  THEN   
									fst_est_del_gmt_dt - shp_pkup_gmt_dt
									ELSE NULL END

		    ,fdat_sdd_diff_hr_val     = CASE WHEN fnl_arpt_dest_gmt_dt > '2000-01-01' AND fnl_arpt_dest_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END

		    ,fdat_sdd_diff_dy_val     = CASE WHEN fnl_arpt_dest_gmt_dt > '2000-01-01' AND fnl_arpt_dest_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
									fnl_arpt_dest_gmt_dt - CAST(scheduled_delivery_gmt_ts AS DATE) 
									ELSE NULL END;
	


		IF ld_end_dt = ld_month_end_dt THEN 

			UPDATE logistics_app.em_carrier_dq_raw_file
			SET compltn_mthly_sent_ind='N'
			WHERE fiscal_dt BETWEEN ld_month_start_dt AND ld_month_end_dt
			AND smmry_rptg_cd='Y' AND event_cd NOT IN ('AP','SD');			
			
			UPDATE logistics_app.em_carrier_dq_raw_file
			SET latency_mthly_sent_ind='N' 
			WHERE B2B_Dt BETWEEN ld_month_start_dt AND ld_month_end_dt
			AND smmry_rptg_cd='Y';			
			
			
			
			UPDATE logistics_app.EM_CARRIER_DQ_RAW_FILE
			SET compltn_mthly_sent_ind='Y'
			WHERE TRIM(ship_from_country_cd)||TRIM(pod_country_cd) NOT IN ('CNUS','CNCA','CNMX','CNBR','CNCO','CNCL','USBR','USCO','USCL','TWUS','TWCA','TWMX','TWBR','TWCO','TWCL')
			AND event_cd IN ('B6','X4','X5','X3','P1')
			AND compltn_mthly_sent_ind='N' 
			AND region_cd IN ('AMR');
		
			UPDATE logistics_app.EM_CARRIER_DQ_RAW_FILE
			SET compltn_mthly_sent_ind='Y'
			WHERE TRIM(ship_from_country_cd)||TRIM(pod_country_cd) NOT IN ('CNUS','CNCA','CNMX','CNCO','CNCL','USCO','USCL','TWUS','TWCA','TWMX','TWCO','TWCL')
			AND event_cd IN ('K1','X6')
			AND compltn_mthly_sent_ind='N' 
			AND region_cd IN ('AMR');
			
			UPDATE logistics_app.EM_CARRIER_DQ_RAW_FILE
			SET compltn_mthly_sent_ind='Y'
			WHERE ship_from_city_name LIKE '%SHENZHEN%'
			AND TRIM(pod_country_cd)= 'HK'
			AND event_cd IN ('B6','X4','X5','X3','P1','K1','X6')
			AND compltn_mthly_sent_ind='N' 
			AND region_cd IN ('PAC');
			
			
			
			UPDATE a FROM logistics_app.em_carrier_dq_raw_file a, logistics.em_carrier_dq_exclusion b
			SET compltn_mthly_sent_ind='Y'
			WHERE 
				a.region_cd=b.region_cd
			AND a.ship_from_country_cd=b.ship_from_country_cd
			AND a.pod_country_cd=b.ship_to_country_cd
			AND a.display_process_type_cd=b.process_type_cd
			AND a.parent_scac_cd=b.scac_cd 
			AND a.carrier_leg_cd=b.carrier_leg_cd
			AND a.event_phase_name=b.event_name 
			AND b.exclude_cd='XO'
			AND a.shipment_mode_cd='Ocean'
			AND a.compltn_mthly_sent_ind='N';
		
		
			
			UPDATE a FROM logistics_app.em_carrier_dq_raw_file a, logistics.em_carrier_dq_exclusion b
			SET compltn_mthly_sent_ind='Y'
			WHERE 
				a.region_cd=b.region_cd
			AND a.ship_from_country_cd=b.ship_from_country_cd
			AND a.pod_country_cd=b.ship_to_country_cd
			AND a.display_process_type_cd=b.process_type_cd
			AND a.parent_scac_cd=b.scac_cd 
			AND a.carrier_leg_cd=b.carrier_leg_cd
			AND a.event_phase_name=b.event_name 
			AND b.exclude_cd='X'
			AND a.compltn_mthly_sent_ind='N';
				
		



				SET  lv_msg = 'Inserting into main table wrk_em_dq_sent_items_mth for PAC and AMR' ;
				CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
		
				DELETE FROM logistics_app.wrk_em_dq_sent_items_mth;
		
				INSERT INTO logistics_app.wrk_em_dq_sent_items_mth
				(
					shipment_id ,
					event_handler_nr,
					parent_scac_cd
				)
				SELECT
					shipment_id,
					event_handler_nr,
					parent_scac_cd
				FROM 
					logistics.em_carrier_dq_raw_file
				WHERE 
						(compltn_mthly_sent_ind='N' 
						OR 
						latency_mthly_sent_ind='N'
						)
					AND region_cd IN ('PAC','AMR')
				GROUP BY  1,2,3
				;

				SET  lv_msg = 'Inserting into main table wrk_em_dq_sent_items_mth for EURO' ;
				CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
				
				INSERT INTO logistics_app.wrk_em_dq_sent_items_mth
				(
					shipment_id ,
					event_handler_nr,
					parent_scac_cd,
					sender_nr
				)
				SELECT
					shipment_id,
					event_handler_nr,
					parent_scac_cd,
					sender_nr
				FROM 
					logistics.em_carrier_dq_raw_file
				WHERE 
					(	compltn_mthly_sent_ind='N' 
						OR 
						latency_mthly_sent_ind='N'
					)
					AND region_cd IN ('EURO')
				GROUP BY  1,2,3,4
				;

		COLLECT STATS ON  logistics_app.wrk_em_dq_sent_items_mth;






				DELETE FROM logistics_app.EM_CARRIER_DQ_MTH_ACCURACY;
		
				INSERT INTO logistics_app.EM_CARRIER_DQ_MTH_ACCURACY
				(
				 shipment_id		 
				,event_handler_nr	 
				,parent_scac_cd		 
				,sender_nr    
				,sender_name	       
				,fiscal_dt		 
				,region_cd 
				,scheduled_delivery_gmt_ts
				,ship_from_country_cd
				,ship_to_country_cd
				,carrier_chg_wt_ind
				,associated_reason_ind					  
				,appt_reg_gmt_dt	 
				,appt_reg_gmt_ts	 
				,appt_reg_b2b_dt
				,appt_reg_b2b_ts		
				,fst_est_del_gmt_dt	 
				,fst_est_del_gmt_ts	 
				,fst_est_del_b2b_dt
				,fst_est_del_b2b_ts
				,lst_est_del_gmt_dt
				,lst_est_del_gmt_ts	 
				,lst_est_del_b2b_dt
				,lst_est_del_b2b_ts	  
				,del_gmt_dt		 
				,del_gmt_ts		 
				,del_b2b_dt
				,del_b2b_ts		 
				,del_appt_gmt_dt	 
				,del_appt_gmt_ts	 
				,del_appt_b2b_dt
				,del_appt_b2b_ts	 
				,fnl_arpt_dest_gmt_dt	 
				,fnl_arpt_dest_gmt_ts	 
				,fnl_arpt_dest_b2b_dt
				,fnl_arpt_dest_b2b_ts	 
				,shp_pkup_gmt_dt	 
				,shp_pkup_gmt_ts	 
				,shp_pkup_b2b_dt
				,shp_pkup_b2b_ts	 
				,report_type_ind
				) 
			SELECT
				ecdr.shipment_id	     
				,ecdr.event_handler_nr	      
				,ecdr.parent_scac_cd	      
				,MAX(ecdr.sender_nr)   
				,MAX(ecdr.sender_name)		  
				,fiscal_dt		 
				,region_cd
				,scheduled_delivery_gmt_ts
				,ship_from_country_cd
				,pod_country_cd
				,MAX(CASE WHEN	chargeable_wt_kg_msr IS NULL AND chargeable_wt_lb_msr IS NULL THEN '0' ELSE '1' END) AS carrier_chg_wt_ind
				,MAX(CASE WHEN event_name IN ('Shipment Delay','Delivery Not Completed') AND reason_cd <>'NS' THEN '1' ELSE '0' END)  AS associated_reason_ind			
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_dt END)
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_ts END)  
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_dt END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_dt END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_received_ts END) 
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_dt	 END)  
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_received_ts END)
				,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_dt END)
				,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_ts END )  
				,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_received_ts END)
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_dt END)
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_ts END)
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_dt END)
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_dt END)
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_ts END)
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_received_ts END)
				,MIN(CASE WHEN compltn_mthly_sent_ind='N' AND latency_mthly_sent_ind='N' THEN 'B'
					WHEN compltn_mthly_sent_ind='N' AND latency_mthly_sent_ind='Y' THEN 'C'
					WHEN compltn_mthly_sent_ind='Y' AND latency_mthly_sent_ind='N' THEN 'L'
					WHEN compltn_mthly_sent_ind='X' AND latency_mthly_sent_ind='N' THEN 'L'
					ELSE NULL END) AS "Report_type_ind"
		   FROM logistics.em_carrier_dq_raw_file ecdr
		   INNER JOIN logistics_app.wrk_em_dq_sent_items_mth wed 
						ON (ecdr.shipment_id=wed.shipment_id AND ecdr.event_handler_nr=wed.event_handler_nr AND ecdr.parent_scac_cd=wed.parent_scac_cd )  
		   WHERE region_cd IN ('PAC','AMR') 
		   GROUP BY 1,2,3,6,7,8,9,10
		   ;	 


		INSERT INTO logistics_app.EM_CARRIER_DQ_MTH_ACCURACY
				(
				 shipment_id		 
				,event_handler_nr	 
				,parent_scac_cd		 
				,sender_nr  
				,sender_name		 
				,fiscal_dt		 
				,region_cd 
				,scheduled_delivery_gmt_ts
				,ship_from_country_cd
				,ship_to_country_cd
				,carrier_chg_wt_ind
				,associated_reason_ind							  
				,appt_reg_gmt_dt	 
				,appt_reg_gmt_ts	 
				,appt_reg_b2b_dt
				,appt_reg_b2b_ts	 
				,fst_est_del_gmt_dt	 
				,fst_est_del_gmt_ts	 
				,fst_est_del_b2b_dt
				,fst_est_del_b2b_ts
				,lst_est_del_gmt_dt
				,lst_est_del_gmt_ts	 
				,lst_est_del_b2b_dt
				,lst_est_del_b2b_ts	 
				,del_gmt_dt		 
				,del_gmt_ts		 
				,del_b2b_dt
				,del_b2b_ts		 
				,del_appt_gmt_dt	 
				,del_appt_gmt_ts	 
				,del_appt_b2b_dt
				,del_appt_b2b_ts	 
				,fnl_arpt_dest_gmt_dt	 
				,fnl_arpt_dest_gmt_ts	 
				,fnl_arpt_dest_b2b_dt
				,fnl_arpt_dest_b2b_ts	
				,shp_pkup_gmt_dt	 
				,shp_pkup_gmt_ts	 
				,shp_pkup_b2b_dt
				,shp_pkup_b2b_ts	 
				,report_type_ind
				) 
			SELECT
				ecdr.shipment_id	     
				,ecdr.event_handler_nr	      
				,ecdr.parent_scac_cd	      
				,ecdr.sender_nr	  
				,ecdr.sender_name	     
				,fiscal_dt		 
				,region_cd
				,scheduled_delivery_gmt_ts
				,ship_from_country_cd
				,pod_country_cd
				,MAX(CASE WHEN	chargeable_wt_kg_msr IS NULL AND chargeable_wt_lb_msr IS NULL THEN '0' ELSE '1' END) AS carrier_chg_wt_ind
				,MAX(CASE WHEN event_name IN ('Shipment Delay','Delivery Not Completed') AND reason_cd <>'NS' THEN '1' ELSE '0' END)  AS associated_reason_ind			
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_dt END)
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_ts END)  
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_dt END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_dt END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_received_ts END) 
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_dt	 END)  
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_received_ts END)
				,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_dt END)
				,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_ts END )  
				,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_received_ts END)
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_dt END)
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_ts END)
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_dt END)
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_dt END)
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_ts END)
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_dt END)
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_received_ts END)
				,MIN(CASE WHEN compltn_mthly_sent_ind='N' AND latency_mthly_sent_ind='N' THEN 'B'
					WHEN compltn_mthly_sent_ind='N' AND latency_mthly_sent_ind='Y' THEN 'C'
					WHEN compltn_mthly_sent_ind='Y' AND latency_mthly_sent_ind='N' THEN 'L'
					ELSE NULL END) AS "Report_type_ind"
		   FROM logistics.em_carrier_dq_raw_file ecdr
		   INNER JOIN logistics_app.wrk_em_dq_sent_items_mth wed 
					ON (ecdr.shipment_id=wed.shipment_id AND ecdr.event_handler_nr=wed.event_handler_nr AND ecdr.sender_nr = wed.sender_nr AND ecdr.parent_scac_cd=wed.parent_scac_cd )  
		   WHERE region_cd IN ('EURO') 
		   GROUP BY 1,2,3,4,5,6,7,8,9,10
		   ;	 
		
		UPDATE logistics_app.em_carrier_dq_mth_accuracy 
	SET
		    del_appt_diff_hr_val      = CASE WHEN del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (del_appt_gmt_ts - del_gmt_ts	DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (del_appt_gmt_ts - del_gmt_ts	 DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (del_appt_gmt_ts - del_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (del_appt_gmt_ts - del_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END
      
		    ,del_appt_diff_dy_val     = CASE WHEN del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									del_appt_gmt_dt -  del_gmt_dt
									ELSE NULL END
	
		    ,appt_req_diff_hr_val     = CASE WHEN appt_reg_gmt_dt > '2000-01-01' AND appt_reg_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
										EXTRACT(DAY FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END
	
		    ,appt_req_diff_dy_val     = CASE WHEN appt_reg_gmt_dt > '2000-01-01' AND appt_reg_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
										appt_reg_gmt_dt - CAST(scheduled_delivery_gmt_ts AS DATE)
										ELSE NULL END

		    ,est_sdd_diff_hr_val      = CASE WHEN fst_est_del_gmt_dt > '2000-01-01' AND fst_est_del_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN
										EXTRACT(DAY FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END
	
		    ,est_sdd_diff_dy_val      = CASE WHEN fst_est_del_gmt_dt > '2000-01-01' AND fst_est_del_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN
										fst_est_del_gmt_dt - CAST(scheduled_delivery_gmt_ts AS DATE)
										ELSE NULL END
	
		    ,est_del_appt_diff_hr_val = CASE WHEN lst_est_del_gmt_dt > '2000-01-01' AND lst_est_del_gmt_dt < '2030-01-01' 
										AND del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (lst_est_del_gmt_ts - del_appt_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (lst_est_del_gmt_ts - del_appt_gmt_ts	 DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (lst_est_del_gmt_ts - del_appt_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (lst_est_del_gmt_ts - del_appt_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END

		    ,est_del_appt_diff_dy_val = CASE WHEN lst_est_del_gmt_dt > '2000-01-01' AND lst_est_del_gmt_dt < '2030-01-01' 
										AND del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01'  THEN   
									lst_est_del_gmt_dt - del_appt_gmt_dt
									ELSE NULL END

		    ,est_pod_diff_hr_val      = CASE WHEN lst_est_del_b2b_dt > '2000-01-01' AND lst_est_del_b2b_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									lst_est_del_b2b_dt - del_gmt_dt
									ELSE NULL END						

		    ,est_pod_diff_dy_val      = CASE WHEN lst_est_del_gmt_dt > '2000-01-01' AND lst_est_del_gmt_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									lst_est_del_gmt_dt - del_gmt_dt 
									ELSE NULL END						

		    ,est_pkup_diff_hr_val     = CASE WHEN fst_est_del_b2b_dt > '2000-01-01' AND fst_est_del_b2b_dt < '2030-01-01' 
										AND shp_pkup_gmt_dt > '2000-01-01' AND shp_pkup_gmt_dt < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts	 DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END

		    ,est_pkup_diff_dy_val     = CASE WHEN fst_est_del_gmt_dt > '2000-01-01' AND fst_est_del_gmt_dt < '2030-01-01' 
										AND shp_pkup_gmt_dt > '2000-01-01' AND shp_pkup_gmt_dt < '2030-01-01'  THEN   
									fst_est_del_gmt_dt - shp_pkup_gmt_dt
									ELSE NULL END

		    ,fdat_sdd_diff_hr_val     = CASE WHEN fnl_arpt_dest_gmt_dt > '2000-01-01' AND fnl_arpt_dest_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END

		    ,fdat_sdd_diff_dy_val     = CASE WHEN fnl_arpt_dest_gmt_dt > '2000-01-01' AND fnl_arpt_dest_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
									fnl_arpt_dest_gmt_dt - CAST(scheduled_delivery_gmt_ts AS DATE) 
									ELSE NULL END;
	
		END IF;
		

		IF ld_end_dt = ld_quarter_end_dt THEN

			UPDATE logistics_app.em_carrier_dq_raw_file
			SET compltn_qtrly_sent_ind ='N' 
			WHERE fiscal_dt BETWEEN ld_quarter_start_dt AND ld_quarter_end_dt 
			AND smmry_rptg_cd='Y' AND event_cd NOT IN ('AP','SD');			
			
			UPDATE logistics_app.em_carrier_dq_raw_file
			SET latency_qtrly_sent_ind='N' 
			WHERE B2B_Dt BETWEEN ld_quarter_start_dt AND ld_quarter_end_dt
			AND smmry_rptg_cd='Y';			
			
			
			
			UPDATE logistics_app.EM_CARRIER_DQ_RAW_FILE
			SET compltn_qtrly_sent_ind='Y'
			WHERE TRIM(ship_from_country_cd)||TRIM(pod_country_cd) NOT IN ('CNUS','CNCA','CNMX','CNBR','CNCO','CNCL','USBR','USCO','USCL','TWUS','TWCA','TWMX','TWBR','TWCO','TWCL')
			AND event_cd IN ('B6','X4','X5','X3','P1')
			AND compltn_qtrly_sent_ind='N'
			AND region_cd IN ('AMR') ;
		
			UPDATE logistics_app.EM_CARRIER_DQ_RAW_FILE
			SET compltn_qtrly_sent_ind='Y'
			WHERE TRIM(ship_from_country_cd)||TRIM(pod_country_cd) NOT IN ('CNUS', 'CNCA', 'CNMX', 'CNCO', 'CNCL', 'USCO', 'USCL','TWUS','TWCA','TWMX','TWCO','TWCL')
			AND event_cd IN ('K1','X6')
			AND compltn_qtrly_sent_ind='N' 
			AND region_cd IN ('AMR');
			
			UPDATE logistics_app.EM_CARRIER_DQ_RAW_FILE
			SET compltn_qtrly_sent_ind='Y'
			WHERE ship_from_city_name LIKE '%SHENZHEN%'
			AND TRIM(pod_country_cd)= 'HK'
			AND event_cd IN ('B6','X4','X5','X3','P1','K1','X6')
			AND compltn_qtrly_sent_ind='N' 
			AND region_cd IN ('PAC');
			
			
		
			
			UPDATE a FROM logistics_app.em_carrier_dq_raw_file a, logistics.em_carrier_dq_exclusion b
			SET compltn_qtrly_sent_ind='Y'
			WHERE 
				a.region_cd=b.region_cd
			AND a.ship_from_country_cd=b.ship_from_country_cd
			AND a.pod_country_cd=b.ship_to_country_cd
			AND a.display_process_type_cd=b.process_type_cd
			AND a.parent_scac_cd=b.scac_cd 
			AND a.carrier_leg_cd=b.carrier_leg_cd
			AND a.event_phase_name=b.event_name 
			AND b.exclude_cd='XO'
			AND a.shipment_mode_cd='Ocean'
			AND a.compltn_qtrly_sent_ind='N';
		
		
			
			UPDATE a FROM logistics_app.em_carrier_dq_raw_file a, logistics.em_carrier_dq_exclusion b
			SET compltn_qtrly_sent_ind='Y'
			WHERE 
				a.region_cd=b.region_cd
			AND a.ship_from_country_cd=b.ship_from_country_cd
			AND a.pod_country_cd=b.ship_to_country_cd
			AND a.display_process_type_cd=b.process_type_cd
			AND a.parent_scac_cd=b.scac_cd 
			AND a.carrier_leg_cd=b.carrier_leg_cd
			AND a.event_phase_name=b.event_name 
			AND b.exclude_cd='X'
			AND a.compltn_qtrly_sent_ind='N';
		
				



				SET  lv_msg = 'Inserting into main table wrk_em_dq_sent_items_qtr for PAC and AMR' ;
				CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
		
				DELETE FROM logistics_app.wrk_em_dq_sent_items_qtr;
		
				INSERT INTO logistics_app.wrk_em_dq_sent_items_qtr
				(
					shipment_id ,
					event_handler_nr,
					parent_scac_cd
				)
				SELECT
					shipment_id,
					event_handler_nr,
					parent_scac_cd
				FROM 
					logistics.em_carrier_dq_raw_file
				WHERE 
						(compltn_qtrly_sent_ind='N' 
						OR 
						latency_qtrly_sent_ind='N'
						)
					AND region_cd IN ('PAC','AMR')
				GROUP BY  1,2,3
				;

				SET  lv_msg = 'Inserting into main table wrk_em_dq_sent_items_qtr for EURO' ;
				CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
				
				INSERT INTO logistics_app.wrk_em_dq_sent_items_qtr
				(
					shipment_id ,
					event_handler_nr,
					parent_scac_cd,
					sender_nr
				)
				SELECT
					shipment_id,
					event_handler_nr,
					parent_scac_cd,
					sender_nr
				FROM 
					logistics.em_carrier_dq_raw_file
				WHERE 
					(	compltn_qtrly_sent_ind='N' 
						OR 
						latency_qtrly_sent_ind='N'
					)
					AND region_cd IN ('EURO')
				GROUP BY  1,2,3,4
				;

		COLLECT STATS ON  logistics_app.wrk_em_dq_sent_items_qtr;






				DELETE FROM logistics_app.em_carrier_dq_qtr_accuracy;
		
				INSERT INTO logistics_app.em_carrier_dq_qtr_accuracy
				(
				 shipment_id		 
				,event_handler_nr	 
				,parent_scac_cd		 
				,sender_nr  
				,sender_name		 
				,fiscal_dt		 
				,region_cd
				,scheduled_delivery_gmt_ts
				,ship_from_country_cd
				,ship_to_country_cd
				,carrier_chg_wt_ind
				,associated_reason_ind			
				,appt_reg_gmt_dt	 
				,appt_reg_gmt_ts	 
				,appt_reg_b2b_dt
				,appt_reg_b2b_ts	 
				,fst_est_del_gmt_dt	 
				,fst_est_del_gmt_ts	 
				,fst_est_del_b2b_dt
				,fst_est_del_b2b_ts
				,lst_est_del_gmt_dt
				,lst_est_del_gmt_ts	 
				,lst_est_del_b2b_dt
				,lst_est_del_b2b_ts	 
				,del_gmt_dt		 
				,del_gmt_ts		 
				,del_b2b_dt
				,del_b2b_ts		 
				,del_appt_gmt_dt	 
				,del_appt_gmt_ts	 
				,del_appt_b2b_dt
				,del_appt_b2b_ts	 
				,fnl_arpt_dest_gmt_dt	 
				,fnl_arpt_dest_gmt_ts	 
				,fnl_arpt_dest_b2b_dt
				,fnl_arpt_dest_b2b_ts	 
				,shp_pkup_gmt_dt	 
				,shp_pkup_gmt_ts	 
				,shp_pkup_b2b_dt
				,shp_pkup_b2b_ts	 
				,report_type_ind
				) 
			SELECT
				ecdr.shipment_id	     
				,ecdr.event_handler_nr	      
				,ecdr.parent_scac_cd	      
				,MAX(ecdr.sender_nr) 
				,MAX(ecdr.sender_name)		    
				,fiscal_dt		 
				,region_cd
				,scheduled_delivery_gmt_ts
				,ship_from_country_cd
				,pod_country_cd
				,MAX(CASE WHEN	chargeable_wt_kg_msr IS NULL AND chargeable_wt_lb_msr IS NULL THEN '0' ELSE '1' END)								AS carrier_chg_wt_ind
				,MAX(CASE WHEN event_name IN ('Shipment Delay','Delivery Not Completed') AND reason_cd <>'NS' THEN '1' ELSE '0' END)				AS associated_reason_ind		  
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_dt END)															AS appt_reg_gmt_dt
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_ts END)															AS appt_reg_gmt_ts
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_dt END)																AS appt_reg_b2b_dt
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_dt END)	AS fst_est_del_gmt_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_ts END)	AS fst_est_del_gmt_ts
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_dt END)			AS fst_est_del_b2b_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_dt END)		AS lst_est_del_gmt_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_ts END)		AS lst_est_del_gmt_ts
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_dt END)			AS lst_est_del_b2b_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_dt	 END)										AS del_gmt_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_ts END)										AS del_gmt_ts
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_dt END)												AS del_b2b_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_received_ts END)
				,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_dt END)															AS del_appt_gmt_dt
				,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_ts END )														AS del_appt_gmt_ts
				,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_dt END)																	AS del_appt_b2b_dt
				,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_received_ts END)
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_dt END)												AS fnl_arpt_dest_gmt_dt
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_ts END)												AS fnl_arpt_dest_gmt_ts
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_dt END)													AS fnl_arpt_dest_b2b_dt
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_dt END)																AS shp_pkup_gmt_dt
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_ts END)																AS shp_pkup_gmt_ts
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_dt END)																	AS shp_pkup_b2b_dt
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_received_ts END)
				,MIN(CASE WHEN compltn_qtrly_sent_ind='N' AND latency_qtrly_sent_ind='N' THEN 'B'
					WHEN compltn_qtrly_sent_ind='N' AND latency_qtrly_sent_ind='Y' THEN 'C'
					WHEN compltn_qtrly_sent_ind='Y' AND latency_qtrly_sent_ind='N' THEN 'L'
					WHEN compltn_qtrly_sent_ind='X' AND latency_qtrly_sent_ind='N' THEN 'L'
					ELSE NULL END) AS "Report_type_ind"
		   FROM logistics.em_carrier_dq_raw_file ecdr
		   INNER JOIN logistics_app.wrk_em_dq_sent_items_qtr wed 
						ON (ecdr.shipment_id=wed.shipment_id AND ecdr.event_handler_nr=wed.event_handler_nr AND ecdr.parent_scac_cd=wed.parent_scac_cd )  
		   WHERE region_cd IN ('PAC','AMR') 
		   GROUP BY 1,2,3,6,7,8,9,10
		   ;	 


		INSERT INTO logistics_app.em_carrier_dq_qtr_accuracy
				(
				 shipment_id		 
				,event_handler_nr	 
				,parent_scac_cd		 
				,sender_nr  
				,sender_name		 
				,fiscal_dt		 
				,region_cd
				,scheduled_delivery_gmt_ts
				,ship_from_country_cd
				,ship_to_country_cd
				,carrier_chg_wt_ind
				,associated_reason_ind			
				,appt_reg_gmt_dt	 
				,appt_reg_gmt_ts	 
				,appt_reg_b2b_dt
				,appt_reg_b2b_ts	 
				,fst_est_del_gmt_dt	 
				,fst_est_del_gmt_ts	 
				,fst_est_del_b2b_dt
				,fst_est_del_b2b_ts
				,lst_est_del_gmt_dt
				,lst_est_del_gmt_ts	 
				,lst_est_del_b2b_dt
				,lst_est_del_b2b_ts	 
				,del_gmt_dt		 
				,del_gmt_ts		 
				,del_b2b_dt
				,del_b2b_ts		
				,del_appt_gmt_dt	 
				,del_appt_gmt_ts	 
				,del_appt_b2b_dt
				,del_appt_b2b_ts	 
				,fnl_arpt_dest_gmt_dt	 
				,fnl_arpt_dest_gmt_ts	 
				,fnl_arpt_dest_b2b_dt
				,fnl_arpt_dest_b2b_ts	 
				,shp_pkup_gmt_dt	 
				,shp_pkup_gmt_ts	 
				,shp_pkup_b2b_dt
				,shp_pkup_b2b_ts	 
				,report_type_ind
				) 
			SELECT
				ecdr.shipment_id	     
				,ecdr.event_handler_nr	      
				,ecdr.parent_scac_cd	      
				,ecdr.sender_nr	 
				,ecdr.sender_name	      
				,fiscal_dt		 
				,region_cd
				,scheduled_delivery_gmt_ts
				,ship_from_country_cd
				,pod_country_cd
				,MAX(CASE WHEN	chargeable_wt_kg_msr IS NULL AND chargeable_wt_lb_msr IS NULL THEN '0' ELSE '1' END)								AS carrier_chg_wt_ind
				,MAX(CASE WHEN event_name IN ('Shipment Delay','Delivery Not Completed') AND reason_cd <>'NS' THEN '1' ELSE '0' END)				AS associated_reason_ind		  
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_dt END)															AS appt_reg_gmt_dt
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN event_gmt_ts END)															AS appt_reg_gmt_ts
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_dt END)																AS appt_reg_b2b_dt
				,MAX(CASE WHEN event_name = 'Appointment Requested' THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_dt END)	AS fst_est_del_gmt_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN event_gmt_ts END)	AS fst_est_del_gmt_ts
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_dt END)			AS fst_est_del_b2b_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_First_Occurrence_Ind = 1 THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_dt END)		AS lst_est_del_gmt_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN event_gmt_ts END)		AS lst_est_del_gmt_ts
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_dt END)			AS lst_est_del_b2b_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Estimated' AND Scac_Last_Occurrence_Ind= 1 THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_dt	 END)										AS del_gmt_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN event_gmt_ts END)										AS del_gmt_ts
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_dt END)												AS del_b2b_dt
				,MAX(CASE WHEN event_name = 'Delivered' AND event_phase_cd = 'Actual' THEN b2b_received_ts END)
				,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_dt END)															AS del_appt_gmt_dt
				,MIN(CASE WHEN event_name = 'Delivery Appointment' THEN event_gmt_ts END )														AS del_appt_gmt_ts
				,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_dt END)																	AS del_appt_b2b_dt
				,MAX(CASE WHEN event_name = 'Delivery Appointment' THEN b2b_received_ts END)
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_dt END)												AS fnl_arpt_dest_gmt_dt
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN event_gmt_ts END)												AS fnl_arpt_dest_gmt_ts
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_dt END)													AS fnl_arpt_dest_b2b_dt
				,MIN(CASE WHEN event_name = 'Final Destination Airport Terminal' THEN b2b_received_ts END)
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_dt END)																AS shp_pkup_gmt_dt
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN event_gmt_ts END)																AS shp_pkup_gmt_ts
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_dt END)																	AS shp_pkup_b2b_dt
				,MAX(CASE WHEN event_name = 'Shipment Picked Up' THEN b2b_received_ts END)
				,MIN(CASE WHEN compltn_qtrly_sent_ind='N' AND latency_qtrly_sent_ind='N' THEN 'B'
					WHEN compltn_qtrly_sent_ind='N' AND latency_qtrly_sent_ind='Y' THEN 'C'
					WHEN compltn_qtrly_sent_ind='Y' AND latency_qtrly_sent_ind='N' THEN 'L'
					WHEN compltn_qtrly_sent_ind='X' AND latency_qtrly_sent_ind='N' THEN 'L'
					ELSE NULL END) AS "Report_type_ind"
		   FROM logistics.em_carrier_dq_raw_file ecdr
		   INNER JOIN logistics_app.wrk_em_dq_sent_items_qtr wed 
					ON (ecdr.shipment_id=wed.shipment_id AND ecdr.event_handler_nr=wed.event_handler_nr AND ecdr.sender_nr = wed.sender_nr AND ecdr.parent_scac_cd=wed.parent_scac_cd )  
		   WHERE region_cd IN ('EURO') 
		   GROUP BY 1,2,3,4,5,6,7,8,9,10
		   ;	 

		UPDATE logistics_app.em_carrier_dq_qtr_accuracy 
	SET
		    del_appt_diff_hr_val      = CASE WHEN del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (del_appt_gmt_ts - del_gmt_ts	DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (del_appt_gmt_ts - del_gmt_ts	 DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (del_appt_gmt_ts - del_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (del_appt_gmt_ts - del_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END
      
		    ,del_appt_diff_dy_val     = CASE WHEN del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									del_appt_gmt_dt -  del_gmt_dt
									ELSE NULL END
	
		    ,appt_req_diff_hr_val     = CASE WHEN appt_reg_gmt_dt > '2000-01-01' AND appt_reg_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
										EXTRACT(DAY FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (appt_reg_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END
	
		    ,appt_req_diff_dy_val     = CASE WHEN appt_reg_gmt_dt > '2000-01-01' AND appt_reg_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
										appt_reg_gmt_dt - CAST(scheduled_delivery_gmt_ts AS DATE)
										ELSE NULL END

		    ,est_sdd_diff_hr_val      = CASE WHEN fst_est_del_gmt_dt > '2000-01-01' AND fst_est_del_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN
										EXTRACT(DAY FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (fst_est_del_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END
	
		    ,est_sdd_diff_dy_val      = CASE WHEN fst_est_del_gmt_dt > '2000-01-01' AND fst_est_del_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN
										fst_est_del_gmt_dt - CAST(scheduled_delivery_gmt_ts AS DATE)
										ELSE NULL END
	
		    ,est_del_appt_diff_hr_val = CASE WHEN lst_est_del_gmt_dt > '2000-01-01' AND lst_est_del_gmt_dt < '2030-01-01' 
										AND del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (lst_est_del_gmt_ts - del_appt_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (lst_est_del_gmt_ts - del_appt_gmt_ts	 DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (lst_est_del_gmt_ts - del_appt_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (lst_est_del_gmt_ts - del_appt_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END

		    ,est_del_appt_diff_dy_val = CASE WHEN lst_est_del_gmt_dt > '2000-01-01' AND lst_est_del_gmt_dt < '2030-01-01' 
										AND del_appt_gmt_dt > '2000-01-01' AND del_appt_gmt_dt < '2030-01-01'  THEN   
									lst_est_del_gmt_dt - del_appt_gmt_dt
									ELSE NULL END

		    ,est_pod_diff_hr_val      = CASE WHEN lst_est_del_b2b_dt > '2000-01-01' AND lst_est_del_b2b_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									lst_est_del_b2b_dt - del_gmt_dt
									ELSE NULL END						

		    ,est_pod_diff_dy_val      = CASE WHEN lst_est_del_gmt_dt > '2000-01-01' AND lst_est_del_gmt_dt < '2030-01-01' 
										AND del_gmt_dt > '2000-01-01' AND del_gmt_dt < '2030-01-01'  THEN   
									lst_est_del_gmt_dt - del_gmt_dt 
									ELSE NULL END						

		    ,est_pkup_diff_hr_val     = CASE WHEN fst_est_del_b2b_dt > '2000-01-01' AND fst_est_del_b2b_dt < '2030-01-01' 
										AND shp_pkup_gmt_dt > '2000-01-01' AND shp_pkup_gmt_dt < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts	 DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (fst_est_del_b2b_ts - shp_pkup_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END

		    ,est_pkup_diff_dy_val     = CASE WHEN fst_est_del_gmt_dt > '2000-01-01' AND fst_est_del_gmt_dt < '2030-01-01' 
										AND shp_pkup_gmt_dt > '2000-01-01' AND shp_pkup_gmt_dt < '2030-01-01'  THEN   
									fst_est_del_gmt_dt - shp_pkup_gmt_dt
									ELSE NULL END

		    ,fdat_sdd_diff_hr_val     = CASE WHEN fnl_arpt_dest_gmt_dt > '2000-01-01' AND fnl_arpt_dest_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
									EXTRACT(DAY FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts DAY(4) TO SECOND(0)))*24.00 +
						EXTRACT(HOUR FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0))) +
						EXTRACT(MINUTE FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/60.00 + 
						EXTRACT(SECOND FROM (fnl_arpt_dest_gmt_ts - scheduled_delivery_gmt_ts  DAY(4) TO SECOND(0)))/3600.00
						ELSE NULL END

		    ,fdat_sdd_diff_dy_val     = CASE WHEN fnl_arpt_dest_gmt_dt > '2000-01-01' AND fnl_arpt_dest_gmt_dt < '2030-01-01' 
										AND CAST(scheduled_delivery_gmt_ts AS DATE) > '2000-01-01' AND CAST(scheduled_delivery_gmt_ts AS DATE) < '2030-01-01'  THEN   
									fnl_arpt_dest_gmt_dt - CAST(scheduled_delivery_gmt_ts AS DATE) 
									ELSE NULL END;
	    END IF;
		
		


		SET  lv_msg = 'Procedure logistics_user.SP_LOAD_EM_CARRIER_DQ_RAW_FILE Completed' ;
		CALL logistics_user.log_process_run_message (lv_err_component,lv_msg,CURRENT_TIMESTAMP(0));
		SET lv_out_status = 'COMPLETED';

END;