FILENAME REFFILE '/home/tcrykken/ABB_stackres_00py.csv';

data _res0	;
	 infile REFFILE delimiter = ',' MISSOVER DSD  firstobs=2 ;
	    informat Confirmation_code $32. ;
	    informat Status $15. ;
	    informat Guest_name $19. ;
	    informat Contact $114. ;
	    informat __of_adults best32. ;
	    informat __of_children best32. ;
	    informat __of_infants best32. ;
	    informat Start_date anydtdte12. ;
	    informat End_date anydtdte12. ;
	    informat __of_nights best32. ;
	    informat Booked anydtdte12. ;
	    informat Listing $64. ;
	    informat Earnings nlnum32. ;
	    
	    format Confirmation_code $32. ;
	    format Status $15. ;
	    format Guest_name $19. ;
	    format Contact $114. ;
	    format __of_adults best12. ;
	    format __of_children best12. ;
	    format __of_infants best12. ;
	    format Start_date yymmdd10. ;
	    format End_date yymmdd10. ;
	    format __of_nights best12. ;
	    format Booked yymmdd10. ;
	    format Listing $64. ;
	    format Earnings nlnum12. ;
	 input
	             Confirmation_code  $
	             Status  $
	             Guest_name  $
	             Contact  $
	             __of_adults
	             __of_children
	             __of_infants
	             Start_date
	             End_date
	             __of_nights
	             Booked
	             Listing  $
	             Earnings
	 ;
	 
run;

/* ******************************************* */
/* SO de-dup process */
/* there are dups on nonidentical guest name, phone number (contact), listing name, reservation status (EG 'past guest','confirmed') */
/* proc sql;select distinct status from _res0;quit; */
data _res_lst_grp (drop=listing status contact guest_name)	;
	length lst_grp $15.	;
	set _res0 (where=(status in ('Confirmed'
								,'Past Guest'
								,'Past guest'
								,'Currently hosti'
								,'Review guest	'
								,'Arriving tomorr'
								,'Arriving in 2 d'
								,'Arriving in 3 d'
								,'Arriving in 4 d'
								,'Arriving in 5 d'
								,'Review guest'
								,'Review guest -'
								,'Awaiting guest')))	;
		if listing in ('Luxury Dtwn WP Condo for 4, Hot Tub, Free Ski Bus'
					  ,'Luxury Main St Condo for 4, Hot Tub & Free Ski Bus'
					  ,'Luxury Downtown Condo + Free Ski Bus'
					  ,'Main St Luxury+Hot Tubs+Walk to Brew, Food & Trail'
					  ,'Luxury on Main St +Hot Tub +Steps to Brew & Trails'
					  ,'Luxury on Main St +Hot Tub &Steps to Brew & Trails'
					  ,'Main St Luxury + Hot Tub & Steps to Brew & Trails'
					  ,'Main Street Luxury- Downtown with Hot Tub & Trails')
			then lst_grp='CRVW_4'	;
		else if listing in ('Luxury Main St Condo for 8, WP Base in 2 Bus Stops'
						   ,'Downtown Luxury + Hot Tubs, 2 Bus Stops to Resort'
						   ,'Luxury Downtown Condo + 2 Bus Stops to Resort'
						   ,'Downtown Luxury + Hot Tubs + Walkable Winter Park'
						   ,'Downtown Luxury, Hot Tubs + Walkable Winter Park')
			then lst_grp='CRVW_8'	;
		else if listing in ('Basecamp Suite + Hot Tub for Mountain Adventures'
							,'Basecamp Suite + Downtown for Mountain Adventures'
							,'Hot Tubs & Downtown Basecamp for MTN Adventures'
							,'Downtown Basecamp+Hot Tubs for Mountain Adventures'
							,'Downtown Basecamp&Hot Tubs for Mountain Adventures'
							,'Basecamp Downtown+Hot Tubs for Mountain Adventures')
			then lst_grp='CRVW_suite'	;
		else if listing in ('A charming home and quiet garden by all Denver')
			then lst_grp='Elati'	;
		else do	;
			put 'NEW LISTING TITLE!' listing	;
			_ERROR_=1	;
		end	;
run	;
proc sort data=_res_lst_grp out=_res_nodupkey nodupkey	;
	by confirmation_code booked _ALL_	;
run	;
/* about 30 res have booked dates differing by one day */
data _res	;
	set _res_nodupkey	;
	by confirmation_code booked	;
	if first.confirmation_code	;
run;
/* EO de-dup process */
/* ******************************************* */