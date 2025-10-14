FILENAME REFFILE '/home/tcrykken/ABB_stackTXHX_00py.csv';

data _tx_raw (drop=var1)   ;
	infile REFFILE delimiter = ',' MISSOVER DSD  firstobs=2 ;
	
    informat VAR1 best32. ;
    informat Amount best32. ;
    informat Cleaning_Fee best32. ;
    informat Confirmation_Code $10. ;
    informat Currency $3. ;
    informat Date mmddyy10. ;
    informat Details $45. ;
    informat Gross_Earnings best32. ;
    informat Guest $18. ;
    informat Host_Fee best32. ;
    informat Listing $52. ;
    informat Nights best32. ;
    informat Occupancy_Taxes best32. ;
    informat Paid_Out best32. ;
    informat Reference $1. ;
    informat Start_Date mmddyy10. ;
    informat Type $21. ;
    
    format VAR1 best12. ;
    format Amount best12. ;
    format Cleaning_Fee best12. ;
    format Confirmation_Code $10. ;
    format Currency $3. ;
    format Date mmddyy10. ;
    format Details $45. ;
    format Gross_Earnings best12. ;
    format Guest $18. ;
    format Host_Fee best12. ;
    format Listing $52. ;
    format Nights best12. ;
    format Occupancy_Taxes best12. ;
    format Paid_Out best12. ;
    format Reference $1. ;
    format Start_Date mmddyy10. ;
    format Type $21. ;

	input
            VAR1
            Date
            Type  $
            Confirmation_Code  $
            Start_Date
            Nights
            Guest  $
            Listing  $
            Details  $
            Reference  $
            Currency  $
            Amount
            Paid_Out
            Host_Fee
            Cleaning_Fee
            Gross_Earnings
            Occupancy_Taxes
;
run;


/* standardize listing name here */
data _tx_lstgrp (drop=listing)	;
	length lst_grp $15.	;
	set _tx_raw	(where=(type='Reservation'))  	;

		if listing in ('Luxury Dtwn WP Condo for 4, Hot Tub, Free Ski Bus'
					  ,'Luxury Main St Condo for 4, Hot Tub & Free Ski Bus'
					  ,'Luxury Downtown Condo + Free Ski Bus'
					  ,'Main St Luxury+Hot Tubs+Walk to Brew, Food & Trail'
					  ,'Luxury on Main St +Hot Tub +Steps to Brew & Trails'
					  ,'Luxury on Main St +Hot Tub &Steps to Brew & Trails'
					  )
			then lst_grp='CRVW_4'	;
		else if listing in ('Luxury Main St Condo for 8, WP Base in 2 Bus Stops'
						   ,'Downtown Luxury + Hot Tubs, 2 Bus Stops to Resort'
						   ,'Luxury Downtown Condo + 2 Bus Stops to Resort'
						   ,'Downtown Luxury + Hot Tubs + Walkable Winter Park')
			then lst_grp='CRVW_8'	;
		else if listing in ('Basecamp Suite + Hot Tub for Mountain Adventures'
							,'Basecamp Suite + Downtown for Mountain Adventures'
							,'Hot Tubs & Downtown Basecamp for MTN Adventures'
							,'Hot Tubs & Main Street Basecamp for MTN Adventures'
							,'Downtown Basecamp+Hot Tubs for Mountain Adventures'
							,'Downtown Basecamp&Hot Tubs for Mountain Adventures')
			then lst_grp='CRVW_suite'	;
		else if listing in ('A charming home and quiet garden by all Denver')
			then lst_grp='Elati'	;
		else do	;
			put 'NEW LISTING TITLE!' listing	;
			_ERROR_=1	;
		end	;
run	;




/* ------- */
/* one time fixes */
data _tx_lstgrp	;
	set _tx_lstgrp	;
	if confirmation_code = 'HM5XBZNNMJ' then nights=52	;
run	;




/* ------- *//* ------- *//* ------- */
/* var1 is an csv import artifact and date is the paid out date, */
/* which can dup a confirmation_code if res is over 2 months */
/* ------- */
/* some records are missing gross earnings and occupancy tax */
/* and withing the confirmation code record group */
/* cause a dup with the record not missing those values */

proc sql	;
	create table _tx_singlepay as
		select distinct *
		from _tx_lstgrp
		group by confirmation_code
		having count(distinct date) eq 1
		order by confirmation_code
	;
/* 			above includes  */
/* 				HMQ8DPDZAY 2 rows, no multimonth, money split across 2 rows - should sum dollars */
	create table _tx_singlepay2 
						(drop=occupancy_taxes gross_earnings host_fee 
						rename=(g=gross_earnings h=host_fee ot=occupancy_taxes)) as
		select distinct
			max(gross_earnings) as g
			,max(host_fee) as h
			,max(occupancy_taxes) as ot
			,*
		from _tx_singlepay
		group by confirmation_code
		order by confirmation_code
	;
	create table _tx_multipay (drop=cleaning_fee rename=(clean=cleaning_fee))  as
		select distinct 
			max(cleaning_fee) as clean
			,*
		from _tx_lstgrp
		group by confirmation_code
		having count(distinct date) gt 1
		order by confirmation_code
	;
/* 			above includes case when*/
/* 				HM5XBZNNMJ multirow, multimonth, pay needs to be summed across rows */
	create table _tx_multipay2 (drop=occupancy_taxes gross_earnings host_fee rename=(g=gross_earnings h=host_fee ot=occupancy_taxes)) as
		select distinct
			max(gross_earnings) as g
			,max(host_fee) as h
			,max(occupancy_taxes) as ot
			,*
		from _tx_multipay
		group by confirmation_code, date
		order by confirmation_code, date
	;
	create table _tx_multipay3 (drop=amount gross_earnings host_fee rename=(s=amount g=gross_earning h=host_fee)) as
		select distinct
			sum(amount) as s
			,sum(gross_earnings) as g
			,sum(host_fee) as h
			,*
		from _tx_multipay2 (drop=date)
		group by confirmation_code
		order by confirmation_code
	;
quit	;
proc sort data=_tx_singlepay2 nodupkey out=_tx_singlepay_fix	;
	by confirmation_code _ALL_	;
run	;
proc sort data=_tx_multipay3 nodupkey out=_tx_multipay_fix	;
	by confirmation_code _ALL_	;
run	;
data _tx	;
	set _tx_multipay_fix
		_tx_singlepay_fix	;
run	;