/* 			ppn 				DEFINES 	earnings/nights  */
/* 			,adj_calppn_ 		DEFINES 	(earnings-cleaning_fee-host_fee)/nights  */
/* 			adj_calppn2		DEFINEd		=gross adj for delta of currently cleaning and charged cleaning */
/* 											 earn-clean-host + clean fee adjustment /nights/* 	CLEANING */
/* 											=(gross-cln_dlta-clean-host_fee)/nights */

/* ********************* DEVELOPMENTS NEEDED **************** */
		
/* 		COMPLETED:switch to adj_calppn_ from ppn in calendaring use */


/* 	AIRBNB CLEANING FEES */
		%let abb4_cln= 90	;
		%let abb8_cln= 130	;
		%let abbS_cln= 59	;
/* 	ACTUAL CLEANING COSTS */
/* 		%let cos4_cln= 60	; */
/* 		%let cos8_cln= 100	; */
/* 		%let cosS_cln= 40	; */
/* 		cleaning fee 082222*/
/* 		crvw_suite = $49 */
/* 		crvw_4 = $80 */
/*		crvw_8 = $120 */


proc sort 
		data=_res 
			 nodupkey
			out=rez_srt	;
	by Confirmation_Code _ALL_	;
run	;

proc sort data=_tx
			 nodupkey
		 out=tx_srt	;
	by Confirmation_Code _ALL_	;
run	;

data full_info_01	;
	merge rez_srt tx_srt	;
	by confirmation_code	;
run	;
/* EO INIT MERGE */


/* data full_info_01	; */
/* 	length lst_grp $15.	; */
/* 	set full_info_01	; */
/*  */
/* 		if listing in ('Luxury Dtwn WP Condo for 4, Hot Tub, Free Ski Bus' */
/* 					  ,'Luxury Main St Condo for 4, Hot Tub & Free Ski Bus' */
/* 					  ,'Luxury Downtown Condo + Free Ski Bus' */
/* 					  ,'Main St Luxury+Hot Tubs+Walk to Brew, Food & Trail') */
/* 			then lst_grp='CRVW_4'	; */
/* 		else if listing in ('Luxury Main St Condo for 8, WP Base in 2 Bus Stops' */
/* 						   ,'Downtown Luxury + Hot Tubs, 2 Bus Stops to Resort' */
/* 						   ,'Luxury Downtown Condo + 2 Bus Stops to Resort' */
/* 						   ,'Downtown Luxury + Hot Tubs + Walkable Winter Park') */
/* 			then lst_grp='CRVW_8'	; */
/* 		else if listing in ('Basecamp Suite + Hot Tub for Mountain Adventures' */
/* 							,'Basecamp Suite + Downtown for Mountain Adventures' */
/* 							,'Hot Tubs & Downtown Basecamp for MTN Adventures') */
/* 			then lst_grp='CRVW_suite'	; */
/* 		else if listing in ('A charming home and quiet garden by all Denver') */
/* 			then lst_grp='Elati'	; */
/* 		else do	; */
/* 			put 'NEW LISTING TITLE!' listing	; */
/* 			_ERROR_=1	; */
/* 		end	; */
/* run	; */


data full_info_02 (drop=msclnfee)	;
	length	adv_days	5
			book_DOW	5	;
	
	set full_info_01 end=eof	;
	
	adv_days=start_date-booked	;
	
/* 		f: WEEKDAYn. - sunday=1, saturday=7 */
	book_DOW=input(put(booked,weekday3.),3.)	;
	
	if missing(earnings) then earnings=amount	;
	if missing(amount) then amount=earnings	;
	
	if missing(__of_nights)
		then do	;
			if missing(nights)
				then nights=end_date-start_date	;
			else __of_nights=nights	;
	end	;
	if missing (nights)
		then do	;
			if missing(__of_nights)
				then nights=end_date-start_date	;
			else nights=__of_nights	;
	end	;
	
	if missing(end_date)
		then end_date=start_date+nights	;

	if missing(cleaning_fee)
		then do	;
			msclnfee+1	;
			if lst_grp='Elati' then cleaning_fee=16	;
			else if lst_grp='CRVW_suite' then cleaning_fee=&abbS_cln.	;
			else if lst_grp='CRVW_4' then cleaning_fee=&abb4_cln.	;
			else if lst_grp='CRVW_8' then cleaning_fee=&abb8_cln.	;
		end	;
	
	if lst_grp='Elati' then cln_delta=0	;
	else if lst_grp='CRVW_suite' then cln_delta=cleaning_fee - &abbS_cln.	;
	else if lst_grp='CRVW_4' then cln_delta=cleaning_fee - &abb4_cln.	;
	else if lst_grp='CRVW_8' then cln_delta=cleaning_fee - &abb8_cln.	;
	
	if missing(host_fee)
		then host_fee=earnings*.031	;
	
	los_weekly=0	;
	los_monthly=0	;
	if nights ge 30
		then los_monthly=1	;
	if 7 le nights le 29
		then los_weekly=1	;
		
	if eof then put "Cleaning Fee missing " msclnfee " times."	;
	
run	;
/*  */
/* proc sql; */
/* 	select distinct */
/* 		cleaning_fee as oi */
/* 		,year(start_date) as yer */
/* 		,count(confirmation_code) */
/* 	from full_info_01 (where=(lst_grp ne "Elati")) */
/* 	group by oi, calculated yer */
/* 	; */
/* quit; */
/*  */
/* proc sql; */
/* 	select distinct */
/* 		missing(earnings) as misear */
/* 		,missing(amount) as misamo */
/* 		,missing(end_date) as misend */
/* 		,missing(__of_nights) as misnum */
/* 		,count(*) as count */
/* 	from full_info_02 */
/* 	group by misear, misamo, misend, misnum */
/* 	; */
/* quit; */


/* ADD SUMMARY VARIABLES *//*MOVE coalesce to data step above */
proc sql	;
	create table full_info_03 as
		select
			 earnings/nights 
				as ppn format=dollar5./*rename eARNpn, use gross_earnings*/
			,(earnings/nights)/avg(earnings/nights) 
				as p_ppnavg format=percent7./*rename p_eARNpnavg, use gross_earnings*/
/* 			,(earnings-cleaning_fee-host_fee)/nights  */
/* 				as adj_calppn_ format =dollar5. */
				
			,(earnings-cleaning_fee-cln_delta-host_fee)/nights
				as adj_calppn_ format=dollar5.

				
				
/* 			,(earnings/nights-(cleaning_fee/nights)-host_fee/nights) */
/* 					/avg(earnings/nights-(cleaning_fee/nights)-host_fee/nights)  */
/* 				as P_adj_calppn_ format =percent7. */
				
				
			,((earnings-cleaning_fee-cln_delta-host_fee)/nights)
					/avg((earnings-cleaning_fee-cln_delta-host_fee)/nights) 
				as P_adj_calppn_ format =percent7.

				
			,*
		from full_info_02
			group by lst_grp
			order by lst_grp
	;
quit	;

/* EO */


/*  */
/*  */
/* UNROLL REZ/TX LEVEL INTO PER-DAY LEVEL */
proc sort 
		data=full_info_03 
		out=rentday_seed	
		nodupkey	;
	by lst_grp confirmation_code start_date _ALL_	;
run	;

data rentdaylevel
		(drop=i)	;
	length	lst_grp $ 15
			confirmation_code $ 32
			rent_week 5
			rent_month 5
			rent_month_n $ 3
			rent_year 5
			rent_date 5
			rent_dow 5
			rent_dow_n $ 14
			start_date 8
			nights 8;
	set rentday_seed	;
	by lst_grp confirmation_code start_date	;
	do i=start_date to start_date+nights-1	;
		rent_date=i	;
		rent_week=week(rent_date)	;
		rent_month=month(rent_date)	;
		rent_month_n=put(rent_date,monname3.)	;
		rent_year=year(rent_date)	;
/* 			f: WEEKDAYw. - sunday=1, saturday=7 */
		rent_dow=input(put(rent_date,weekday.),5.)	;
/* 			f: WEEKDATEw. - sasdate - DOWname */
		rent_dow_n=put(rent_date,weekdate3.)	;
		output	;
	end	;
	format 	rent_date mmddyy8.
	 		ppn dollar5.
			adj_calppn_ dollar5.	;
run	;

proc sort data=rentdaylevel	;
	by lst_grp rent_date	;
run	;

/* proc means data=rentdaylevel; */
/* 	by lst_grp	; */
/* 	var adj_calppn_	; */
/* 	class rent_dow rent_dow_n	; */
/* run; */