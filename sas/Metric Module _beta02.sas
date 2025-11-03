/*****************************************************************************************************************
 
SAS file name: Standard_Header.sas
File location: The location of the SAS file
__________________________________________________________________________________________________________________
 
Purpose: 
Author: 
Creation Date: 05/23/23


Some text explaining the program. Perhaps an example call of a macro/module

__________________________________________________________________________________________________________________
 
TO DO:
- subavppn: REMOVE SUBAVERAGE BOOKINGS FROM PREDICTED CALENDAR PPN
modify the calendar PPN to produce rates more in line with the WP market
	-initial idea: create a PPN then remove/modify all bookings below that rate and
		rerun the calendar rate process to create a PPN wort sub-average bookimgs
-
add in crosswalks from
C:\Users\dell\Documents\305 Crestview\305 Analysis\Data\Cross Walks\FSF BotW Check No Payee xwalk
.../home/tcrykken/BotW desc cat xwalk.csv

-
really could use that tool to evaluate special offers for guests asking for
longer term stays etc

-
Q: does model currently factor historical and current cleaning
fees etc into displayed calendar price?


__________________________________________________________________________________________________________________
 
CHANGES:
 
Date: Date of first modification of the code
Modifyer name: Name of the programmer who modified the code
Description: Shortly describe the changes made to the program
 
*****************************************************************************************************************/



/* --------------------------------- */
/* --------------------------------- */
/* 		WHATS BROKEN?!! */
/* 	- */
/* 	-	!!!		cal_price HAS to include the outlier cleaned mos and wk avg_P_calppn insted of just below calc */
/* 	-	!!!			the trick is to pick the dollar cal ppn to use as a base for all the percent based calcs */
/* 	- 	!!!		cal_price=max(week_avg_adj_calppn,mos_avg_adj_calppn)*dow_avg_P_adj_calppn*pric_multi */
/* [ ]	convert holiday dates to datetime for the calendar PROC */


/* 	-	Should P_ppn be a outlier-removed metric (percent-of-average-price-per-night) */
/* 	-	Eventually past holiday booking prices should be identified and removed or normalized  */
/* 			to stop these from spiking a, say average monthly price */
/* --------------------------------- */

/* --------------------------------- */
/* --------------------------------- */
/* MACROS */
/* --------------------------------- */

/* 	DISCOUNT RATES week/month */
/* 		4: 17/33 */
/* 		8: 17/35 */
/* 		suite: 17/27 */
/*		discount rates perhaps should be 1-occupancy rate*/
/* 		this is how I arrived at 9 percent */
/* 		but programmatically, this should follow season or something */
%let wdisc_c4=0.19	;
%let mdisc_c4=0.33	;
%let wdisc_c8=0.19	;
%let mdisc_c8=0.33	;
%let wdisc_su=0.19	;
%let mdisc_su=0.33	;

/* 	CLEANING */
/* 	AIRBNB CLEANING FEES */
%let abb4_cln= 90	;
%let abb8_cln= 130	;
%let abbS_cln= 59	;
/* 	ACTUAL CLEANING COSTS */
/* 		%let cos4_cln= 100	; */
/* 		%let cos8_cln= 150	; */
/* 		%let cosS_cln= 70	; */

/* --------------------------------- */
/* --------------------------------- */




/******************** ODS Reporting BS *********/
options nocenter nodate;
run;
ods pdf 
	file='/home/tcrykken/Metric Module_beta_odstoc.pdf' 
	startpage=no 
	CONTENTS=YES
	style=rtf;
ods noproctitle;
title ;
RUN;
		ODS PROCLABEL= "Metric Module &sysdate9."	;




/* --------------------------------- */
/* 	DEVELOPMENTS 2do: AND THIS IS COMPLETED */
/* --------------------------------- */
/* 	-How to import CRVW305_GM Gholiday calendar level  */
/* 		into pricing calendar for categorical holiday analysis */

/* GOAL: price 50+ days out */
/* --------------------------------- */
/* The NonLin_PppnXAdvDays Modelling program has been used to model the P-ppn x nights out trend */

/* PREP */
/* (A1) need: average booking price
		 , percent of average 
		 		by <week/date unit> of year */
/* (A2) need: price 
				by M-F day of week */
/* (B ) need: percent of average price 
				by days out */
/* 			today is day 0, day 50 is 50 days out */

/* 	(C ) need: estimated monthly gross per listing per month	l */
/* 		can do gross per month wort listing */
/* 		what is the avg for one night per night per listing? */

/* CALCULATION is: 
	(  ) avg booking price * percent of avergae price/days out */

/* --------------------------------------- */


/* 	(C ) need: estimated monthly gross per listing per month	l */
/* 		can do gross per month wort listing */
/* 		what is the avg ppn for one night per month per listing? */
/* 		average nights per month per listing (how to ... with 3058 & 3054/A mix?)  */
		ODS PROCLABEL= 'Gross per Month; 3058, 3054, 305suite'	;
proc sql	;
	select distinct
		year(rent_date) as Year 
		,month(rent_date) as Month
		,sum(ppn) as Gross format dollar7. 
		,sum(adj_calppn_) label='SUM of Adj Cal PPN' format dollar7.
		,avg(p_ppnavg) label='Avg Gross P' format percent.
	from	(select distinct
				rent_date
				,ppn
				,adj_calppn_
				,p_ppnavg
				,confirmation_code
					from rentdaylevel (where=(lst_grp ^= 'Elati'))
	 		)
	group by calculated month,calculated year
	order by calculated month,calculated year
	;
quit	;

/* 		ODS PROCLABEL = 'Gross Per Year; CRVW'	; */
/* 	title 'Gross Per Year; CRVW'	; */
/* proc sql	; */
/* 	create table gross_year as */
/* 		select distinct */
/* 			year(start_date) as Year */
/* 			,sum(earnings) as Gross */
/* 		from full_info_03 */
/* 	group by calculated year */
/* 	order by year */
/* 	; */
/* quit	; */
/* 			 */
/* proc sgplot data=gross_year	; */
/* 	vbar Year / response=Gross	; */
/* run	; */

	ODS PROCLABEL= 'Occupancy Rates';
	title 'count distinct rent_dates OVER max-min rent_dates';
proc sql	;
	select distinct
		 lst_grp
		,count(distinct rent_date)/(max(rent_date)-min(rent_date)) as occ_rate label='Occupancy Rate' format percent7.
	from rentdaylevel (where=(lst_grp ^= 'Elati'))
		group by lst_grp
	;
quit;	
	title;
proc sql noprint	;
	create table occu_rate as
		select distinct
			year(rent_date) as year label='Year'
			,month(rent_date) as month label='Month'
			,compress(put(year(rent_date),5.)||put(month(rent_date),z2.)) as yyyymm
			,count(distinct rent_date)/30.5 as occ_rate label='Occupancy Rate' format percent7.
		from rentdaylevel (where=(lst_grp ^= 'Elati'))
			group by calculated month, calculated year
			order by calculated yyyymm
	;
	title ;
quit	;

	ODS PROCLABEL= 'Occupancy Rate'	;
proc sgplot data=occu_rate	;
	vbar yyyymm / response=occ_rate	;
run	;

	ODS PROCLABEL= 'OCCU_Rate -univariate';
proc univariate data=occu_rate	;
	var occ_rate	;
run	;
/* proc freq data=occu_rate	; */
/* 	tables occ_rate/all	out=occu_stat	; */
/* ; */
/* run	; */


	ODS PROCLABEL= 'Avg CalPpN/Night/Lisitng';
	title 'Avg CalPpN/Night/Lisitng';
proc sql	;
	select distinct
		lst_grp label='Listing'
		,year(rent_date) as year label='Year'
		,month(rent_date) as month label='Month'
		,count(distinct rent_date)/30 as occ_rate label='Occupancy Rate' format percent7.
		,avg(adj_calppn_) label= 'Avg Cal Price' format dollar7.
		,put(avg(adj_calppn_)-std(adj_calppn_),5.)
				||', '||put(avg(adj_calppn_)+std(adj_calppn_),5.)
			label='+/- 1 StD Cal Price Range'
	from rentdaylevel (where=(lst_grp ^= 'Elati'))
		group by lst_grp, calculated month, calculated year
		order by lst_grp, calculated month, calculated year
	;
title ;
quit	;



/* 	AVERAGE LENGTH OF STAY */
ODS PROCLABEL= 'average length of stay';
title 'average length of stay';

proc sql;
	select distinct
		lst_grp
		,avg(__of_nights) format 5.1 as alos
	from (select distinct
			confirmation_code
			,lst_grp
			,__of_nights
				from full_info_03)
	where lst_grp ne 'Elati'
	group by lst_grp
	;
quit;

proc sql;
	select distinct
		lst_grp
		,month(start_date) as smonth
		,avg(__of_nights) format 5.1 as alos
	from (select distinct
			confirmation_code
			,lst_grp
			,start_date
			,__of_nights
				from full_info_03)
	where lst_grp ne 'Elati'
	group by lst_grp, calculated smonth
	;
quit;

ODS PROCLABEL='';
title ;




 
/*  ------------------------------ */
/*  ------------------------------ */
/*  ------------------------------ */
/*  AVERAGE PER MONTH, DOW AND WEEK-OF-YEAR-N */
/* 	TO DO: these three average per month, dow and week-of-year-N  */
/* 	calculations may need outlier elimination (now have this?) */

/* 		--- calculate average unit PPN to use to as */
/* 		--- a price base to calculate cal_price */
/* 		--- rather than a more varying per month or week average unit PPN */
/* 		--- these with the outlier excepted unit month or week average P-PPN */
ods proclabel 'Avg_Adj_CalPPN';
title 'Avg_Adj_CalPPN';
proc sql	;
	select distinct	
		lst_grp
		,avg(adj_calppn_) as Avg_Adj_CalPPN
			  into :lst_grp1 - :lst_grp4 notrim, :appn1 - :appn4
	from rentdaylevel
		group by  lst_grp
	;
/* 			--- below currently puts average PPN seemingly too high --- */
/* 		select distinct	 */
/* 			lst_grp */
/* 			,avg(adj_calppn_) */
/* 				  into :lst_grpO1 - :lst_grpO3 notrim, :appn_O1 - :appn_O3 */
/* 		from rentdaylevel (where=(lst_grp ne 'Elati' */
/* 									and nights le 5 */
/* 									and adv_days gt 3 */
/* 									and P_adj_calppn_ between &p5_beta. and &p95_beta.)) */
/* 			group by  lst_grp */
/* 		; */
quit	;
%put &lst_grp1 &appn1;
title;

/* 		I COULDNT GET THE RIGHT OUTPUT FOR lstmos AND avg_clean */
/* proc sql; */
/* 	select distinct */
/* 		lst_grp */
/* 		,month(rent_date) as month label='Month' */
/* 		,put(coalesce(avg(cleaning_fee),0),dollar5.) */
/* 			into :lstmo1 - :lstmo48 notrim, :acln1 - :acln48 */
/* 	from rentdaylevel */
/* 		group by lst_grp, calculated month */
/* 	; */
/* quit	; */
/*  */
/* %put &lstmo35. &acln35.	; */


proc univariate 
		data=rentdaylevel 
			(where=(lst_grp ne 'Elati'
						and nights le 5
						and adv_days gt 3))
		outtable=uni_ppn_beta
		noprint;
	var P_adj_calppn_	;
run	;
data _NULL_;
	set uni_ppn_beta	;
		call symput('p5_beta',_P5_);
		call symput('p95_beta',_P95_);
run;
proc sql	;
	create table av_pmos_byunit as
		select distinct
			lst_grp
			,rent_month_n
			,rent_month
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,avg(adj_calppn_) as avg_adj_calppn_ format dollar5.
			,count(*) as N
			,std(adj_calppn_) as std_adj_calppn format=5.
			,compress('('||put(min(adj_calppn_),10.)||','||put(max(adj_calppn_),10.)||')') as minmax_adj_calppn
			,avg(cleaning_fee) as avg_clean_fee format dollar5.
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and nights le 5
									and adv_days gt 3
									and P_adj_calppn_ between &p5_beta. and &p95_beta.))
		group by lst_grp, rent_month
		order by lst_grp, rent_month
	;
		create table av_pmos_combo as
		select distinct
			 rent_month_n
			,rent_month
			,count(*) as N
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,std(P_adj_calppn_) as std_P_adj_calppn_ format=percent7.
			,compress('('||put(min(P_adj_calppn_),percent7.)||','||put(max(P_adj_calppn_),percent7.)||')') as minmax_P_adj_calppn_
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and nights le 5
									and adv_days gt 3
									and P_adj_calppn_ between &p5_beta. and &p95_beta.))
		group by rent_month
		order by rent_month
	;
quit	;

/* ****************** */
ods proclabel 'av_pmos_combo'	;
title 'av_pmos_combo';
proc sgplot data=av_pmos_combo	;
	vbar rent_month /response=avg_P_adj_calppn_ stat=median	;
run;

/* ********************** */
/* ********************** */
/* DEVELOPMENT: remove per lst_grp DOW trending in calendar */
/* 	replace with DOW price trending from only 305a 1 night bookings */
/* ********************** */

/* OLD DOW PRICE TREND PROCESS */
/* ********************** */
/* proc sql	; */
/* 	create table av_pday as */
/* 		select distinct */
/* 			lst_grp */
/* 			,rent_dow_n */
/* 			,rent_dow */
/* 			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7. */
/* 			,avg(adj_calppn_) as avg_adj_calppn_ format dollar5. */
/* 			,count(*) as N */
/* 			,std(adj_calppn_) as std_adj_calppn format=5. */
/* 			,compress('('||put(min(adj_calppn_),10.)||','||put(max(adj_calppn_),10.)||')') as minmax_adj_calppn */
/* 		from rentdaylevel (where=(los_weekly=0 and los_monthly=0)) */
/* 		group by lst_grp, rent_dow */
/* 		order by lst_grp, rent_dow */
/* 	; */
/* quit	; */

/* ********************** */
/* ********************** */
/* 	SO new DOW weight  */
/* ********************** */
/* 	development sandbox is priceXtemporal sandbox.sas */
/* ********************** */
ODS PROCLABEL= 'RentDayLevel,Suite,1 Night Bookings - CalPPN UNIVARIATE';
title 'RentDayLevel,Suite,1 Night Bookings - CalPPN UNIVARIATE';
proc univariate 
		data=rentdaylevel 
			(where=(lst_grp='CRVW_suite' and nights=1))
		outtable=uni_ppn;
	var adj_calppn_	;
	histogram adj_calppn_	;
	CDFPLOT adj_calppn_	;
run	;
/* remove outlier notes */
/* 	remove all plusminus 3 sd */
/* 	POTENTIAL DEVELOPMENT: replace outliers with nearest inboard value? */
/* 	For smaller samples of data 2 sd 95percent */
/* 	3 sd 99percent */
data _NULL_	;
	set uni_ppn	;
	call symput('P01',_P1_)	;
	call symput('P99',_P99_);
run	;

proc univariate 
		data=rentdaylevel 
			(where=(lst_grp ne 'Elati'))
		outtable=uni_P_ppn;
	var P_adj_calppn_;
	histogram P_adj_calppn_	;
	CDFPLOT P_adj_calppn_	;
run	;
data _NULL_;
	set uni_P_ppn;
	call symput('Pppn_P5',_P5_);
	call symput('Pppn_P95',_P95_);
run;
%put &Pppn_P5. &Pppn_P95. ;

proc sql	;
	create table av_pday_1nite as
		select distinct
			lst_grp
			,rent_dow_n
			,rent_dow
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,avg(adj_calppn_) as avg_adj_calppn_ format dollar5.
			,count(*) as N
			,std(adj_calppn_) as std_adj_calppn format=5.
			,compress('('||put(min(adj_calppn_),10.)||','||put(max(adj_calppn_),10.)||')') as minmax_adj_calppn
		from rentdaylevel (where=(nights=1))
		group by lst_grp, rent_dow
		order by lst_grp, rent_dow
	;
	create table av_pday_1nite_allCrvw as
		select distinct
			 rent_dow_n
			,rent_dow
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,avg(adj_calppn_) as avg_adj_calppn_ format dollar5.
			,count(*) as N
			,std(adj_calppn_) as std_adj_calppn format=5.
			,compress('('||put(min(adj_calppn_),10.)||','||put(max(adj_calppn_),10.)||')') as minmax_adj_calppn
		from rentdaylevel (where=(nights=1 and lst_grp ne 'Elati' and P_adj_calppn_ between &Pppn_P5. and &Pppn_P95.))
		group by rent_dow
		order by rent_dow
	;
quit	;

/* ****************** */
ods proclabel 'av_pday_1nite'	;
title 'av_pday_1nite';
proc sgplot data=av_pday_1nite	;
	vbar rent_dow /response=avg_P_adj_calppn_ stat=median	;
run;
ods proclabel 'av_P_ppn_pday_1nite 5-95 outlier removed, all CRVW'	;
title 'av_P_ppn_pday_1nite 5-95 outlier removed, all CRVW';
proc sgplot data=av_pday_1nite_allCrvw	;
	vbar rent_dow /response=avg_P_adj_calppn_ stat=median	;
run;
title;

proc sql	;
	create table normm as
		select 
			 wt.avg_P_adj_calppn_
			,in.*
		from
			rentdaylevel (where=(lst_grp='CRVW_suite' and nights=1)) as in
				left join
			av_pday_1nite (where=(lst_grp='CRVW_suite')) as wt
		on
			in.rent_dow eq wt.rent_dow
	;
quit	;

data normm_outl_3sd	;
	set normm (where=(adj_calppn_ between &P01. and &P99.))	;
/* 	thats 3 sd range */
run	;
ODS PROCLABEL 'normm_outl dataset THREE SD outlier removed adj_calppn_ variable';
title 'normm_outl dataset THREE SD outlier removed adj_calppn_ variable';
proc sgplot data=normm_outl_3sd;
	ods output sgplot=sg111;
	vbar rent_dow /response=adj_calppn_ stat=mean 	;
run	;
proc sql;
	create table av_pday as
		select
			 *
			,_Mean1_adj_calppn__/avg(_Mean1_adj_calppn__) format percent7. as avg_P_adj_calppn_
		from sg111
	;
quit	;
/*
rent_dow	_Mean1_adj_calppn__	avg_P_adj_calppn_
	1		$93					102%
	2		$85					93%
	3		$89					98%
	4		$89					98%
	5		$88					97%
	6		$94					103%
	7		$101				111%
*/
/* ********************** */
/* ********************** */
/* 	EO new DOW weight  */
/* ********************** */

/*	ChatGPT produced weekly trend rates:
| ---------------------------------------|
| Day of Week | Percent of Average Price |
| ----------- | ------------------------ |
| Monday      | 65%                      |
| Tuesday     | 62%                      |
| Wednesday   | 75%                      |
| Thursday    | 85%                      |
| Friday      | 100%                     |
| Saturday    | 110%                     |
| Sunday      | 90%                      |
| ---------------------------------------|
*/
data av_P_ppn_pwday_cGPT	;
	input  @1 rent_dow 3. @8 avg_P_adj_calPpn_	;
	datalines;
1      .90
2      .65
3      .62
4      .75
5      .85
6      1.00
7      1.1
	;
run;
proc sql;select avg(avg_P_adj_calPpn_) from av_P_ppn_pwday_cGPT;quit;
proc sql; select .8386*avg_P_adj_calPpn_ as adj, avg_P_adj_calPpn_, rent_dow from av_P_ppn_pwday_cGPT; quit;

proc sql	;
	create table av_pweekN_byunit as
		select distinct
			lst_grp
			,rent_week
			,min(rent_date) as dow_1st format date9.
			,avg(adj_calppn_) as avg_adj_calppn_ format dollar5.
/* 			,count(*) as N */
/* 			,std(adj_calppn_) as std_adj_calppn format=5. */
/* 			,compress('('||put(min(adj_calppn_),10.)||','||put(max(adj_calppn_),10.)||')') as minmax_adj_calppn */
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and nights le 5
									and adv_days gt 3
									and P_adj_calppn_ between &p5_beta. and &p95_beta.))
		group by lst_grp, rent_week
		order by lst_grp, rent_week
	;
	create table avcln_pweekN_byunit as
		select distinct
			lst_grp
			,rent_week
			,min(rent_date) as dow_1st format date9.
			,avg(cleaning_fee) as avg_cln_fee format dollar5.
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and nights le 5
									and adv_days gt 3
									and P_adj_calppn_ between &p5_beta. and &p95_beta.
									and cleaning_fee ne 0))
		group by lst_grp, rent_week
		order by lst_grp, rent_week
	;
	create table avcln_pmosN_byunit as
		select distinct
			lst_grp
			,rent_month
			,avg(cleaning_fee) as avg_cln_fee format dollar5.
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and nights le 5
									and adv_days gt 3
									and P_adj_calppn_ between &p5_beta. and &p95_beta.
									and cleaning_fee ne 0))
		group by lst_grp, rent_month
		order by lst_grp, rent_month
	;
	create table av_pweekN_combo as
		select distinct
			 rent_week
			,min(rent_date) as dow_1st format date9.
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and nights le 5
									and adv_days gt 3
									and P_adj_calppn_ between &p5_beta. and &p95_beta.))
		group by rent_week
		order by rent_week
	;
quit	;


/* need: percent of average price by days out */

ODS PROCLABEL= 'full_info_03 p_ppnavg BY adv_days per listing'	;
proc sgpanel
		data=full_info_03	;
	panelby lst_grp	;
	reg x=adv_days y=p_ppnavg /cli clm	;
run	;

/* MODELLING */
/* proc reg  */
/* 		data=full_info_03  */
/* 			(where=(lst_grp^='Elati' and los_weekly=0 and los_monthly=0)) */
/* 		outest=pXdays_REG; */
/* 	model P_adj_calppn_ = adv_days	; */
/* run	; */

data fully_forx2model	;
	set full_info_03	;
	adv_days2=adv_days**2	;
run;
/* the below does a y=i+a*x+b*x^2+e model */
ODS PROCLABEL= 'p_adj_cal_ppn X adv_days/adv_days^2 y=i+a*x+b*x^2+e model for days out prediction'	;
proc reg data=fully_forx2model plots=predictions(X=adv_days)	;
	model p_adj_calppn_ = adv_days adv_days2 /vif;
run;
/* intercept	0.89567 */
/* adv_days	0.01531 */
/* adv_days2	-0.00014301 */
/* for this model f'(x)=0 where x=54 */
/* the price multiplier on day 54 is 1.30539284 */

/* proc genmod data=full_info_03; */
/* 	model p_adj_calppn_ = adv_days/ dist = normal */
/* link = log noscale; */
/* run; */

/* THIS model is not in use in the program currently */
/* proc reg */
/* 		data=full_info_03 */
/* 			(where=(lst_grp^='Elati')) */
/* 		outest=pXbookDOW	; */
/* 	model P_adj_calppn_ = book_DOW	; */
/* run	; */

/* 	this is the parametric model estimated in the NonLin_PppnXAdvDays Modelling program */
/* y=i+a*x+a*x^2+e */
data p_pricXdaysout (keep=cal_date pric_multi days_out)	;
	
	length cal_date 5	;
	format cal_date mmddyy10.	;

	a=0.01531	;
	a2=-0.00014301	;
	i=0.89567	;
	
	do days_out=1 to 250	;
/* 		if days_out < 100 then pric_multi=i+a*days_out+a2*days_out**2	; MOVED TO MAX_MULTIPLIER MODEL from below:*/
		if days_out < 54 then pric_multi=i+a*days_out+a2*days_out**2	;
/* 		if days_out>= 100 then pric_multi=1	; */
		if days_out>= 54 then pric_multi=1.30539284	;
		cal_date=today()+days_out	;
		output	;
	end	;
run	;
/* EO MODELLING */


/* (B ) need: percent of average price 
				by days out */
/* 			today is day 0, day 50 is 50 days out */

/* ACTION: */
/* merge price metrics onto the 0-100 dates and bob's your uncle */

/* data p_pricXdaysout (keep=cal_date pric_multi days_out)	; */
/* 	set pXdays_REG	; */
/* 	 */
/* 	length cal_date 5	; */
/* 	format cal_date mmddyy10.	; */
/* 	 */
/* 	do days_out=1 to 150	; */
/* 		pric_multi=adv_days*days_out+intercept	; */
/* 		cal_date=today()+days_out	; */
/* 		output	; */
/* 	end	; */
/* run	; */

/* MAKE observed avg rent by listing, extrapolated 100 days out */
proc sql	;
	create table crvw4_future00 as
		select distinct
		 	 d.*
			,c2.rent_month_n
		 	,c1.rent_week
			,c3.rent_dow
			,c2.avg_adj_calppn_ as mos_avg_adj_calppn
			,c1.avg_adj_calppn_ as week_avg_adj_calppn
			,c2.avg_clean_fee as mos_avg_clean_fee
/* 			,c3.avg_adj_calppn_ as dow_avg_adj_calppn */
			,c4.avg_P_adj_calppn_ as mos_avg_P_adj_calppn
			,c5.avg_P_adj_calppn_ as week_avg_P_adj_calppn
			,c3.avg_P_adj_calppn_ as dow_avg_P_adj_calppn
		from p_pricXdaysout as d
			left join
			 av_pweekN_byunit (where=(lst_grp='CRVW_4')) as c1
				on week(d.cal_date) = c1.rent_week
			left join
			 av_pweekN_combo as c5
				on week(d.cal_date) = c5.rent_week
			left join
			 av_pmos_byunit (where=(lst_grp='CRVW_4')) as c2
				on month(d.cal_date) = c2.rent_month
			left join
			 av_pmos_combo as c4
				on month(d.cal_date) = c4.rent_month
			left join
			 av_P_ppn_pwday_cGpt as c3
				on weekday(d.cal_date) = c3.rent_dow
/* 			 av_pday as c3 */
/* 				on weekday(d.cal_date) = c3.rent_dow MOVED TO ChatGPT weekday demand*/
	;
	create table crvwsuite_future00 as
		select distinct
		 	 d.*
			,c2.rent_month_n
		 	,c1.rent_week
			,c3.rent_dow
			,c2.avg_adj_calppn_ as mos_avg_adj_calppn
			,c2.avg_clean_fee as mos_avg_clean_fee
			,c1.avg_adj_calppn_ as week_avg_adj_calppn
/* 			,c3.avg_adj_calppn_ as dow_avg_adj_calppn */
			,c4.avg_P_adj_calppn_ as mos_avg_P_adj_calppn
			,c5.avg_P_adj_calppn_ as week_avg_P_adj_calppn
			,c3.avg_P_adj_calppn_ as dow_avg_P_adj_calppn
		from p_pricXdaysout as d
			left join
			 av_pweekN_byunit (where=(lst_grp='CRVW_suite')) as c1
				on week(d.cal_date) = c1.rent_week
			left join
			 av_pweekN_combo as c5
				on week(d.cal_date) = c5.rent_week
			left join
			 av_pmos_byunit (where=(lst_grp='CRVW_suite')) as c2
				on month(d.cal_date) = c2.rent_month
			left join
			 av_pmos_combo as c4
				on month(d.cal_date) = c4.rent_month
			left join
			 av_P_ppn_pwday_cGpt as c3
				on weekday(d.cal_date) = c3.rent_dow
/* 			 av_pday as c3 */
/* 				on weekday(d.cal_date) = c3.rent_dow MOVED TO ChatGPT weekday demand*/
	;
	create table crvw8_future00 as
		select distinct
		 	 d.*
			,c2.rent_month_n
		 	,c1.rent_week
			,c3.rent_dow
			,c2.avg_adj_calppn_ as mos_avg_adj_calppn
			,c2.avg_clean_fee as mos_avg_clean_fee
			,c1.avg_adj_calppn_ as week_avg_adj_calppn
/* 			,c3.avg_adj_calppn_ as dow_avg_adj_calppn */
			,c4.avg_P_adj_calppn_ as mos_avg_P_adj_calppn
			,c5.avg_P_adj_calppn_ as week_avg_P_adj_calppn
			,c3.avg_P_adj_calppn_ as dow_avg_P_adj_calppn
		from p_pricXdaysout as d
			left join
			 av_pweekN_byunit (where=(lst_grp='CRVW_8')) as c1
				on week(d.cal_date) = c1.rent_week
			left join
			 av_pweekN_combo as c5
				on week(d.cal_date) = c5.rent_week
			left join
			 av_pmos_byunit (where=(lst_grp='CRVW_8')) as c2
				on month(d.cal_date) = c2.rent_month
			left join
			 av_pmos_combo as c4
				on month(d.cal_date) = c4.rent_month
			left join
			 av_P_ppn_pwday_cGpt as c3
				on weekday(d.cal_date) = c3.rent_dow
/* 			 av_pday as c3 */
/* 				on weekday(d.cal_date) = c3.rent_dow MOVED TO ChatGPT weekday demand*/
	;
quit	;


/* CALCULATION is: 
	(  ) avg booking price * percent of avergae price/days out */
/* take avg rental by weekN and apply weekly trend to make a per day expected rent price */

/* 	DISCOUNT RATES week/month macro variables */
/*      --set near program header  */

/* 	CLEANING macro variables */
/*		-- set near program header	*/


%macro weekprice(invar,outvar)	;
	&outvar.=(&invar.+lag1(&invar.)+lag2(&invar.)+lag3(&invar.)+lag4(&invar.)
						+lag5(&invar.)+lag6(&invar.))	;
%mend weekprice	;
%macro monthprice(invar,outvar)	;
	&outvar.=(&invar.+lag1(&invar.)+lag2(&invar.)+lag3(&invar.)+lag4(&invar.)
					+lag5(&invar.)+lag6(&invar.)+lag7(&invar.)+lag8(&invar.)
					+lag9(&invar.)+lag10(&invar.)+lag11(&invar.)+lag12(&invar.)
					+lag13(&invar.)+lag14(&invar.)+lag15(&invar.)+lag16(&invar.)
					+lag17(&invar.)+lag18(&invar.)+lag19(&invar.)+lag20(&invar.)
					+lag21(&invar.)+lag22(&invar.)+lag23(&invar.)+lag24(&invar.)
					+lag25(&invar.)+lag26(&invar.)+lag27(&invar.))	;
%mend monthprice	;

/* 	AND calculate weekly/monthly moving averages */
data crvw4_future01	;
	format _dow dollar7.
		   _wk dollar7.
		   _mos dollar7.
		   cal_priceO dollar7.
		   cal_price dollar7.
		   cal_price_daysouttrend dollar7.
		   cal_price_mean dollar7.
		   discount $20.	
		   week_price dollar7.
		   weekly_disc dollar7.
		   week_disc_price dollar7.
		   weekly_disc_rate dollar7.	
		   monthly_price dollar7.
		   monthly_disc dollar7.
		   monthly_disc_price dollar7.
		   monthly_disc_rate dollar7.
		   ;
		   
	set crvw4_future00	;
	
		   
	weekly_disc=1-&wdisc_c4.	;
	monthly_disc=1-&mdisc_c4.	;
	
/* 		cal_priceO is the outlier eliminated, global week and month trend calcd price */
	cal_priceO=max(week_avg_P_adj_calppn,mos_avg_P_adj_calppn)*&appn1.*dow_avg_P_adj_calppn*pric_multi	;
/* 		WHY is MAX the function used? */
	cal_price=max(week_avg_adj_calppn,mos_avg_adj_calppn)*dow_avg_P_adj_calppn*pric_multi	;
	cal_price_daysouttrend=cal_price*pric_multi	;
	cal_price_mean=mean(week_avg_adj_calppn,mos_avg_adj_calppn)*dow_avg_P_adj_calppn*pric_multi	;
	
	_dow=.;
	_wk=mean(week_avg_adj_calppn,mos_avg_adj_calppn)*week_avg_P_adj_calppn*pric_multi	;
	_mos=mean(mos_avg_adj_calppn,mos_avg_adj_calppn)*mos_avg_P_adj_calppn*pric_multi	;

	%weekprice(cal_price,week_price)	;
	week_disc_price=week_price*weekly_disc	;
	weekly_disc_rate=week_disc_price/7	;
	if _N_ lt 7
		then do	;
			week_price=.	;
			week_disc_price=.	;
			weekly_disc_rate=.	;
		end	;
	%monthprice(cal_price,monthly_price)	;
	monthly_disc_price=monthly_price*monthly_disc	;
	monthly_disc_rate=monthly_disc_price/28	;
	if _N_ lt 28
		then do	;
			monthly_price=.	;
			monthly_disc_price=.	;
			monthly_disc_rate=.	;
		end	;
	calpristr=compress(put(cal_price,5.)||'/'||put(cal_price_daysouttrend,5.)||'/'||put(cal_price_mean,5.))	;
	calpristr2=compress(put(cal_price,5.)||'/'||put(cal_priceO,5.)||'/'|| put(cal_price_daysouttrend,5.));
	discount=compress(put(weekly_disc_rate,5.)||'/'||put(monthly_disc_rate,5.)
				||','||put(week_disc_price,5.)||'/'||put(monthly_disc_price,5.))	;
run	;

/* proc expand  */
/* 		data = crvw4_future01 */
/* 		out = moveavg	; */
/* 	convert cal_price = week_rate */
/* 			/ method = none */
/* 			transformout = (cmovave 7); */
/* run; */



data crvw8_future00	;
	format _dow dollar7.
		   _wk dollar7.
		   _mos dollar7.
		   cal_price dollar7.
		   cal_priceO dollar7.
		   cal_price_daysouttrend dollar7.
		   cal_price_mean dollar7.
		   discount $20.	
		   week_price dollar7.
		   weekly_disc dollar7.
		   week_disc_price dollar7.
		   weekly_disc_rate dollar7.	
		   monthly_price dollar7.
		   monthly_disc dollar7.
		   monthly_disc_price dollar7.
		   monthly_disc_rate dollar7.
		   ;
	set crvw8_future00	;
	
	weekly_disc=1-&wdisc_c8.	;
	monthly_disc=1-&mdisc_c8.	;
	
/* 		cal_priceO is the outlier eliminated, global week and month trend calcd price */
	cal_priceO=max(week_avg_P_adj_calppn,mos_avg_P_adj_calppn)*&appn2.*dow_avg_P_adj_calppn*pric_multi	;
/* 		WHY is MAX the function used? */
	cal_price=max(week_avg_adj_calppn,mos_avg_adj_calppn)*dow_avg_P_adj_calppn*pric_multi	;
	cal_price_daysouttrend=cal_price*pric_multi	;
	cal_price_mean=mean(week_avg_adj_calppn,mos_avg_adj_calppn)*dow_avg_P_adj_calppn*pric_multi	;
	
	_dow=.;
	_wk=mean(week_avg_adj_calppn,mos_avg_adj_calppn)*week_avg_P_adj_calppn*pric_multi	;
	_mos=mean(mos_avg_adj_calppn,mos_avg_adj_calppn)*mos_avg_P_adj_calppn*pric_multi	;

	%weekprice(cal_price,week_price)	;
	week_disc_price=week_price*weekly_disc	;
	weekly_disc_rate=week_disc_price/7	;
	if _N_ lt 7
		then do	;
			week_price=.	;
			week_disc_price=.	;
			weekly_disc_rate=.	;
		end	;
	%monthprice(cal_price,monthly_price)	;
	monthly_disc_price=monthly_price*monthly_disc	;
	monthly_disc_rate=monthly_disc_price/28	;
	if _N_ lt 28
		then do	;
			monthly_price=.	;
			monthly_disc_price=.	;
			monthly_disc_rate=.	;
		end	;
	calpristr=compress(put(cal_price,5.)||'/'||put(cal_price_daysouttrend,5.)||'/'||put(cal_price_mean,5.))	;
	discount=compress(put(weekly_disc_rate,5.)||'/'||put(monthly_disc_rate,5.)
				||','||put(week_disc_price,5.)||'/'||put(monthly_disc_price,5.))	;
run	;

data crvwsuite_future00	;
	format _dow dollar7.
		   _wk dollar7.
		   _mos dollar7.
		   cal_price dollar7.
		   cal_priceO dollar7.
		   cal_price_daysouttrend dollar7.
		   cal_price_mean dollar7.
		   discount $20.	
		   week_price dollar7.
		   weekly_disc dollar7.
		   week_disc_price dollar7.
		   weekly_disc_rate dollar7.	
		   monthly_price dollar7.
		   monthly_disc dollar7.
		   monthly_disc_price dollar7.
		   monthly_disc_rate dollar7.
		   ;
	set crvwsuite_future00	;
	
	weekly_disc=1-&wdisc_su.	;
	monthly_disc=1-&mdisc_su.	;
	
/* 		cal_priceO is the outlier eliminated, global week and month trend calcd price */
	cal_priceO=max(week_avg_P_adj_calppn,mos_avg_P_adj_calppn)*&appn3.*dow_avg_P_adj_calppn*pric_multi	;
/* 		WHY is MAX the function used? */
	cal_price=max(week_avg_adj_calppn,mos_avg_adj_calppn)*dow_avg_P_adj_calppn*pric_multi	;
	cal_price_daysouttrend=cal_price*pric_multi	;
	cal_price_mean=mean(week_avg_adj_calppn,mos_avg_adj_calppn)*dow_avg_P_adj_calppn*pric_multi	;
	
	_dow=.;
	_wk=mean(week_avg_adj_calppn,mos_avg_adj_calppn)*week_avg_P_adj_calppn*pric_multi	;
	_mos=mean(mos_avg_adj_calppn,mos_avg_adj_calppn)*mos_avg_P_adj_calppn*pric_multi	;

	%weekprice(cal_price,week_price)	;
	week_disc_price=week_price*weekly_disc	;
	weekly_disc_rate=week_disc_price/7	;
	if _N_ lt 7
		then do	;
			week_price=.	;
			week_disc_price=.	;
			weekly_disc_rate=.	;
		end	;
	%monthprice(cal_price,monthly_price)	;
	monthly_disc_price=monthly_price*monthly_disc	;
	monthly_disc_rate=monthly_disc_price/28	;
	if _N_ lt 28
		then do	;
			monthly_price=.	;
			monthly_disc_price=.	;
			monthly_disc_rate=.	;
		end	;
	calpristr=compress(put(cal_price,5.)||'/'||put(cal_price_daysouttrend,5.)||'/'||put(cal_price_mean,5.))	;
	discount=compress(put(weekly_disc_rate,5.)||'/'||put(monthly_disc_rate,5.)
				||','||put(week_disc_price,5.)||'/'||put(monthly_disc_price,5.))	;
	
run	;

/* 	CRVW 8 pricing as sum of CRVW4 and Suite */
proc sql	;
	create table crvw8_future_new as
		select
			 c4.cal_date
			,c4.cal_priceO+cs.cal_priceO as cal_priceO format=dollar5.
		from  crvw4_future01 as c4
			 ,crvwsuite_future00 as cs
			where cs.cal_date=c4.cal_date
		order by cal_date
	;
quit	;

data crvw8_future00_ppn	;
	set crvw8_future00	;
run;

proc sort data=crvw8_future00;
	by cal_date	;
data crvw8_future00;
	merge crvw8_future_new (rename=(cal_priceO=combo_price))
		  crvw8_future00	;
	by cal_date	;
run;



data holidays (drop=tday yeer)	;
	
/* 	calc today and current year.   */
	tday=today()	;
	yeer=year(today())	;
/* 	calc holiday curr year.   */
/* 	if today gt holi curr year  */
/* 		then recalc holiday for next year */
	BOXING = holiday('BOXING',yeer)	;
		if BOXING lt tday
			then BOXING=holiday('BOXING',yeer+1)	;
	CHRISTMAS = holiday('CHRISTMAS', yeer)	;
		if CHRISTMAS lt tday
			then CHRISTMAS=holiday('CHRISTMAS',yeer+1)	;
	COLUMBUS = holiday('COLUMBUS', yeer)	;
		if COLUMBUS lt tday
			then COLUMBUS=holiday('COLUMBUS',yeer+1)	;
	EASTER = holiday('EASTER', yeer)	;
		if EASTER lt tday
			then EASTER=holiday('EASTER',yeer+1)	;
	FATHERS = holiday('FATHERS', yeer)	;
		if FATHERS lt tday
			then FATHERS=holiday('FATHERS',yeer+1)	;
	HALLOWEEN = holiday('HALLOWEEN', yeer)	;
		if HALLOWEEN lt tday
			then HALLOWEEN=holiday('HALLOWEEN',yeer+1)	;
	LABOR = holiday('LABOR', yeer)	;
		if LABOR lt tday
			then LABOR=holiday('LABOR',yeer+1)	;
	MLK = holiday('MLK', yeer)	;
		if MLK lt tday
			then MLK=holiday('MLK',yeer+1)	;
	MEMORIAL = holiday('MEMORIAL', yeer)	;
		if MEMORIAL lt tday
			then MEMORIAL=holiday('MEMORIAL',yeer+1)	;
	MOTHERS = holiday('MOTHERS', yeer)	;
		if MOTHERS lt tday
			then MOTHERS=holiday('MOTHERS',yeer+1)	;
	NEWYEAR = holiday('NEWYEAR', yeer)	;
		if NEWYEAR lt tday
			then NEWYEAR=holiday('NEWYEAR',yeer+1)	;
	THANKSGIVING = holiday('THANKSGIVING', yeer)	;
		if THANKSGIVING lt tday
			then THANKSGIVING=holiday('THANKSGIVING',yeer+1)	;
	USINDEPENDENCE = holiday('USINDEPENDENCE', yeer)	;
		if USINDEPENDENCE lt tday
			then USINDEPENDENCE=holiday('USINDEPENDENCE',yeer+1)	;
	USPRESIDENTS = holiday('USPRESIDENTS', yeer)	;
		if USPRESIDENTS lt tday
			then USPRESIDENTS=holiday('USPRESIDENTS',yeer+1)	;
	VALENTINES = holiday('VALENTINES', yeer)	;
		if VALENTINES lt tday
			then VALENTINES=holiday('VALENTINES',yeer+1)	;
	VETERANS = holiday('VETERANS', yeer)	;
		if VETERANS lt tday
			then VETERANS=holiday('VETERANS',yeer+1)	;
	VETERANSUSG = holiday('VETERANSUSG', yeer)	;
		if VETERANSUSG lt tday
			then VETERANSUSG=holiday('VETERANSUSG',yeer+1)	;
	VETERANSUSPS = holiday('VETERANSUSPS', yeer)	;
		if VETERANSUSPS lt tday
			then VETERANSUSPS=holiday('VETERANSUSPS',yeer+1)	;
run	;
proc transpose
		data=holidays
		out=trnsp_holi
			(rename=(col1=sta _NAME_=act))	;
	var _ALL_	;
run	;

/* this allows more horizontal space within the calendar printout */
options linesize=200; /*80, 120, 132, 200 are possible*/

ODS PROCLABEL=	'crvw8_future00 combo'	;
title	"crvw8_future00"	;
title2	"&lst_grp2. average adj_calppn_= &appn2."	;

footnote 'cal_price_8'	;
footnote2 'crvw4 and suite combo price'	;
proc calendar data=crvw8_future00 holidata=trnsp_holi	;
	start cal_date	;
	var combo_price	;
	mean mos_avg_clean_fee / f=dollar5.	;
	holistart sta;
	holivar act;
run	;
footnote2;

ODS PROCLABEL=	'crvw8_future00 PPN'	;
footnote2 'crvw4 and suite PPN rate'	;
proc calendar data=crvw8_future00_ppn holidata=trnsp_holi	;
	start cal_date	;
	var cal_priceO ;
	mean mos_avg_clean_fee / f=dollar5.	;
	holistart sta;
	holivar act;
run	;
footnote;footnote2;


ODS PROCLABEL=	'crvw4_future01'	;
title "crvw4_future01"	;
title2	" &lst_grp1. average adj_calppn_ = &appn1."	;
footnote "	cal_priceO=max(week_avg_P_adj_calppn,mos_avg_P_adj_calppn)*&appn1.*dow_avg_P_adj_calppn*pric_multi"	;
proc calendar data=crvw4_future01 holidata=trnsp_holi	;
	start cal_date	;
	var cal_priceO	;
	mean mos_avg_clean_fee / f=dollar5.	;
	holistart sta;
	holivar act;
run	;

ODS PROCLABEL=	'crvwsuite_future00'	;
title "crvwsuite_future00"	;
title2	" &lst_grp3. average adj_calppn_ = &appn3."	;
footnote "	cal_priceO=max(week_avg_P_adj_calppn,mos_avg_P_adj_calppn)*&appn3.*dow_avg_P_adj_calppn*pric_multi"	;
proc calendar data=crvwsuite_future00 holidata=trnsp_holi	;
	start cal_date	;
	var cal_priceO	;
	mean mos_avg_clean_fee / f=dollar5.	;
	holistart sta;
	holivar act;
run	;
title	;title2	;
footnote;footnote3;


/**************************/
/**************************/
/*  Past Unique PPN by MMDD and lst_grp  */
/* 		PPN value is (earnings-cleaning_fee-host_fee)/nights  */
/* 		cleaning fee is total cleaning fee for booking */
/* 		the PPN and cleaning fee lists are <ordered and related>? and with most recent year first */
/**************************/


proc sql	;
	create table dsnct_ppn as
		select distinct
			substr(put(rent_date,mmddyy8.),1,5) as mmmdd
			,catx('f','d') as wknum_dow
			,strip(input(put(week(rent_date),z2.),$2.))
/* 			strip(input(put(weekday(rent_date),z2.),$2.)) */
			,*
		from rentdaylevel
			order by lst_grp, calculated mmmdd, rent_date, confirmation_code
	;
quit	;
data dppn_str	;
	set dsnct_ppn	;
	by lst_grp mmmdd rent_date confirmation_code	;
	if last.confirmation_code	;
run	;

data dppn_str (drop=adj_calppn_)	;
	set dppn_str	;
	by lst_grp mmmdd	;
	
/* 	dummy a prospective calendar date */
	mm=month(rent_date)	;
	dd=day(rent_date)	;
	yy=year(today())	;
	if mm=2 and dd=29 then dd=28	;
	cal_date=mdy(mm,dd,yy)	;
	format cal_date mmddyy10.	;
	
	retain ppn_str cln_str	;
	length ppn_str $200. cln_str $200.	;
	if first.mmmdd then 
		do	; ppn_str=""	;	cln_str=""	;	end;
	ppn_str=catx(',',trim(ppn_str),put(adj_calppn_,5.))	;
	cln_str=catx(',',trim(cln_str),put(cleaning_fee,5.))	;
	if last.mmmdd then output	;
run	;

proc sort data=dppn_str	;
	by lst_grp cal_date	;
run	;

options linesize=96;
ODS PROCLABEL='Booked Price History';
title	'Booked Price History'	;
footnote	'value is Calendar Price, (earnings-cleaning_fee-host_fee)/nights '	;
footnote2	'cleaning fee is total cleaning fee for booking'	;
footnote3	'the PPN and cleaning fee lists are <ordered and related>? and with most recent year first' ;

	proc calendar data=dppn_str;
		by lst_grp	;
		start cal_date	;
		var ppn_str	;
		var cln_str	;
	run	;
	
title	;
footnote	;footnote2	;footnote3	;

ods pdf close;
options linesize=64;