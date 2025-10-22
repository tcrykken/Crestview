options mprint;


	
	proc sql	;
		select distinct	
			lst_grp
			,avg(adj_calppn_)
				  into :lst_grp1 - :lst_grp4 notrim, :appn1 - :appn4
		from rentdaylevel
			group by  lst_grp
		;
		select distinct	
			lst_grp
			,avg(adj_calppn_)
				  into :lst_grpO1 - :lst_grpO3 notrim, :appn_O1 - :appn_O3
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and nights le 5
									and adv_days gt 3
									and P_adj_calppn_ between &p5_beta. and &p95_beta.))
			group by  lst_grp
		;
		
	quit	;
	%put &lst_grp1. &appn1.	&lst_grpO1. &appn_O1;
	
	/* 	AND calculate weekly/monthly moving averages */
data crvw4_future_beta	;
	format _dow dollar7.
		   _wk dollar7.
		   _mos dollar7.
		   cal_price dollar7.
		   cal_price_beta dollar7.
		   cal_price_betaO dollar7.
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
	
/* 		Why is DAY-OF-WEEK-proportion-PPN used as the WEIGHT on the PRICE */
/* 		WHY is MAX the function used? */



	cal_price=max(week_avg_adj_calppn,mos_avg_adj_calppn)*dow_avg_P_adj_calppn*pric_multi	;

	cal_price_beta=max(week_avg_P_adj_calppn,mos_avg_P_adj_calppn)*&appn1.*dow_avg_P_adj_calppn*pric_multi	;
	cal_price_betaO=max(week_avg_P_adj_calppn,mos_avg_P_adj_calppn)*&appn_O1.*dow_avg_P_adj_calppn*pric_multi	;



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

ODS PROCLABEL=	'crvw4_future_beta'	;
title 'crvw4_future_beta'	;
proc calendar data=crvw4_future_beta
		(where=(cal_date between '01may2022'd and '31may2022'd))
/* 		(where=(cal_date between '01aug2022'd and '31aug2022'd)) */
		;
	start cal_date	;
	var cal_price cal_price_beta cal_price_betaO	;
run	;





title 'freq nights stayed -all noElati';
proc freq 
		data=full_info_03 
			(where=(lst_grp ne 'Elati'));
	table nights	;
run	;
title 'adv_days, P-ppn -no Elati';
proc sgplot data=full_info_03 
			(where=(lst_grp^='Elati' and los_monthly=0 and los_weekly=0))	;
	vbar adv_days /response=P_adj_calppn_ stat=median	;
run	;
title 'adv_days,freq -no Elati';
proc sgplot data=full_info_03 
			(where=(lst_grp^='Elati'))	;
	vbar adv_days	;
run	;



title 'uni ppn - suite, 1 night';
proc univariate 
		data=rentdaylevel 
			(where=(lst_grp='CRVW_suite' and nights=1))
		outtable=uni_ppn;
	var adj_calppn_	;
	histogram adj_calppn_	;
	CDFPLOT adj_calppn_	;
run	;
title 'uni P ppn - suite, 1 night';
proc univariate 
		data=rentdaylevel 
			(where=(lst_grp='CRVW_suite' and nights=1))
		outtable=uni_Pppn;
	var P_adj_calppn_	;
run	;
/* remove outlier notes */
/* 	remove all plusminus 3 sd */
/* 	replace outliers with nearest inboard value */
/* 	For smaller samples of data 2 sd 95percent */
/* 	2 sd 99percent */
/* 	4 standard deviations 99point9 */

/* Given mu and sigma identify outliers  */
/* compute a z-score for every xi  */
/* xi is the number of standard deviations away from the mean  */
/* Data values that have a z-score sigma  */
/* greater than a threshold are outliers */
data _NULL_	;
	set uni_ppn	;
	call symput('ppn3sd',_STD_*3)	;
	call symput('ppnMean',_MEAN_)	;
	call symput('P01',_P1_)	;
	call symput('P99',_P99_);
run	;
data _NULL_	;
	set uni_Pppn	;
	call symput('Pppn3sd',_STD_*3)	;
	call symput('PppnMean',_MEAN_)	;
%put &ppn3sd. &ppnMean.	;
%put %sysevalf(&ppnMean. - &ppn3sd.) and %sysevalf(&ppnMean. + &ppn3sd.)	;
%put &Pppn3sd.= &PppnMean.=	;

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
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and nights=1))
		group by lst_grp, rent_dow
		order by lst_grp, rent_dow
	;
quit	;



proc sql	;
	create table av_pday_305a1niteNoOut as
		select distinct
			 rent_dow_n
			,rent_dow
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,avg(adj_calppn_) as avg_adj_calppn_ format dollar5.
			,count(*) as N
			,std(adj_calppn_) as std_adj_calppn format=5.
			,compress('('||put(min(adj_calppn_),10.)||','||put(max(adj_calppn_),10.)||')') as minmax_adj_calppn
		from rentdaylevel (where=(lst_grp='CRVW_suite' 
								and nights=1
								and adj_calppn_ between  %sysevalf(&ppnMean. - &ppn3sd.) and %sysevalf(&ppnMean. + &ppn3sd.)))
		group by rent_dow
		order by rent_dow
	;
quit	;


/* OUTLIER develop outlier flagging with 3 sd and mean values */
/* ----------- */
/* 	move this up into av_pday_1nite SQL above */
/* ----------- *//* ----------- *//* ----------- */
/* select */
/* 	* */
/* 	,case when adj_calppn_ outside 3sd range 1 */
/* 		end else 0 as as sd3outlier */
/* 	from rentdaylevel (where=(nights=1)) */



title'first look at ppn w/o doing anything';
proc sgplot data=rentdaylevel 
			(where=(lst_grp='CRVW_suite' and nights=1))	;
	vbar rent_dow /response=adj_calppn_ stat=mean	;
run	;
proc sql	;
	create table normm as
		select 
			in.adj_calppn_ * (1/wt.avg_P_adj_calppn_) as m1
			,wt.avg_P_adj_calppn_
			,in.*
		from
			rentdaylevel (where=(lst_grp='CRVW_suite' and nights=1)) as in
				left join
			av_pday_1nite (where=(lst_grp='CRVW_suite')) as wt
		on
			in.rent_dow eq wt.rent_dow
	;
quit	;
title 'normm dataset m1 metric  ppn times one over percent avg ppn';
proc sgplot data=normm	;
	vbar rent_dow /response=m1 stat=mean	;
run;
title 'normm dataset adj_calppn_ var';
proc sgplot data=normm	;
	vbar rent_dow /response=adj_calppn_ stat=mean	;
run	;
	
	
/* ----------- *//* ----------- *//* ----------- */
/* outlier take one */
data normm_outl_2sd	;
	set normm (where=(adj_calppn_ between 38.70 and 94.16))	;
/* 	thats 2 sd range */
run	;
title 'normm_outl dataset 2 SD outlier removed adj_callppn_ variable';
proc sgplot data=normm_outl_2sd	;
	vbar rent_dow /response=adj_calppn_ stat=mean	;
run	;

/* outlier take 2 */
data normm_outl_3sd	;
	set normm (where=(adj_calppn_ between &P01. and &P99.))	;
/* 	thats 3 sd range */
run	;
title 'normm_outl dataset THREE SD outlier removed adj_calppn_ variable';
proc sgplot data=normm_outl_3sd;
	ods output sgplot=sg111;
	vbar rent_dow /response=adj_calppn_ stat=mean 	;
run	;
proc sql;
	create table DOW_wt as
		select
			 *
			,_Mean1_adj_calppn__/avg(_Mean1_adj_calppn__) format percent7. as DOW_wt
		from sg111
	;
quit	;
title 'normm_outl dataset THREE SD outlier removed p_ppnavg variable';
/* this results in a non 100 percent average over DOW result  */
/*  because the precursors are from outside the 305a 1 nite pool */
proc sgplot data=normm_outl_3sd;
	ods output sgplot=dow_avgPppn_305a1nite;
	vbar rent_dow /response=P_ppnavg stat=mean;
run	;
title;















/* ----------- *//* ----------- *//* ----------- */
/* ----------- *//* ----------- *//* ----------- */
/* ----------- *//* ----------- *//* ----------- */

/* outlier work to do with normalizing month and week of year */

/* ----------- *//* ----------- *//* ----------- */

proc sql	;
	create table av_pmos_B as
		select distinct
			 lst_grp
			,rent_month_n
			,rent_month
			,count(*) as N
			,avg(adj_calppn_) as avg_adj_calppn_ format=5.
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,std(P_adj_calppn_) as std_P_adj_calppn_ format=percent7.
			,compress('('||put(min(P_adj_calppn_),percent7.)||','||put(max(P_adj_calppn_),percent7.)||')') as minmax_P_adj_calppn_
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and los_weekly=0 and los_monthly=0))
		group by lst_grp, rent_month
		order by lst_grp, rent_month
	;
	create table av_pmos_Bnew as
		select distinct
			 rent_month_n
			,rent_month
			,count(*) as N
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,std(P_adj_calppn_) as std_P_adj_calppn_ format=percent7.
			,compress('('||put(min(P_adj_calppn_),percent7.)||','||put(max(P_adj_calppn_),percent7.)||')') as minmax_P_adj_calppn_
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and los_weekly=0 and los_monthly=0))
		group by rent_month
		order by rent_month
	;
quit	;

/* proc univariate  */
/* 		data=rentdaylevel */
/* 			(where=(lst_grp ne 'Elati' */
/* 					and rent_month=3))	; */
/* 	var P_adj_calppn_	; */
/* 	histogram P_adj_calppn_	; */
/* run; */




title 'uni ppn - all, exp wk, mos length';
proc univariate 
		data=rentdaylevel 
			(where=(lst_grp ne 'Elati'
					and los_weekly=0 and los_monthly=0))
		outtable=uni_ppn_nwkmos;
	var P_adj_calppn_	;
	histogram P_adj_calppn_	;
	cdf P_adj_calppn_	;
run	;
data _NULL_;
	set uni_ppn_nwkmos	;
		call symput('p1_nwm',_P1_);
		call symput('p99_nwm',_P99_);
		call symput('p5_nwm',_P5_);
		call symput('p95_nwm',_P95_);
run;
title ',uni ppn - 1 to 4 los, adv_days 12plus, ';
proc univariate 
		data=rentdaylevel 
			(where=(lst_grp ne 'Elati'
						and nights le 5
						and adv_days gt 11))
		outtable=uni_ppn_beta;
	var P_adj_calppn_	;
	histogram P_adj_calppn_	;
	cdf P_adj_calppn_	;
run	;
data _NULL_;
	set uni_ppn_beta	;
		call symput('p1_beta',_P1_);
		call symput('p99_beta',_P99_);
		call symput('p5_beta',_P5_);
		call symput('p95_beta',_P95_);
run;

%put &p1_nwm &p99_nwm &p5_nwm &p95_nwm	;
%put &p1_beta &p99_beta &p5_beta &p95_beta	;
title;

proc sql	;
	create table av_pmos_BnO as
		select distinct
			 rent_month_n
			,rent_month
			,count(*) as N
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,std(P_adj_calppn_) as std_P_adj_calppn_ format=percent7.
			,compress('('||put(min(P_adj_calppn_),percent7.)||','||put(max(P_adj_calppn_),percent7.)||')') as minmax_P_adj_calppn_
 		from rentdaylevel (where=(lst_grp ne 'Elati'
									and nights le 5
									and adv_days gt 11
									and P_adj_calppn_ between &p1_nwm. and &p99_nwm.))
		group by rent_month
		order by rent_month
	;
		create table av_pmos_Bn1 as
		select distinct
			 lst_grp
			,rent_month_n
			,rent_month
			,count(*) as N
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,std(P_adj_calppn_) as std_P_adj_calppn_ format=percent7.
			,compress('('||put(min(P_adj_calppn_),percent7.)||','||put(max(P_adj_calppn_),percent7.)||')') as minmax_P_adj_calppn_
		from rentdaylevel (where=(lst_grp ne 'Elati'
									and nights le 5
									and adv_days gt 3
									and P_adj_calppn_ between &p5_beta. and &p99_beta.))
		group by rent_month
		order by rent_month
	;
quit	;



/* ****************** */
ods proclabel 'av_pmos - all, exp wk, mos length'	;
title 'av_pmos - all, exp wk, mos length';
proc sgplot data=av_pmos_B	;
	vbar rent_month /response=avg_P_adj_calppn_ stat=mean	;
run;
ods proclabel 'av_pmos - 1 to 4 nights, 3sd outlier removed, ge12 adv_days'	;
title 'av_pmos - 1 to 4 nights, 3sd outlier removed, ge12 adv_days';
proc sgplot data=av_pmos_BnO	;
	vbar rent_month /response=avg_P_adj_calppn_ stat=mean	;
run;
ods proclabel 'av_pmos - 1 to 5 nights, 2sd (different) outlier removed, ge3 adv_days'	;
title 'av_pmos - 1 to 5 nights, 2sd (different) outlier removed, ge3 adv_days';
proc sgplot data=av_pmos_Bn1	;
	vbar rent_month /response=avg_P_adj_calppn_ stat=mean	;
run;



/* proc sql	; */
/* 	select distinct */
/* 		lst_grp */
/* 		,avg(host_fee/earnings) as ahfpd */
/* 		,avg(host_fee/nights) as hfpn */
/* 		,avg(cleaning_fee) as avcln */
/* 	from full_info_03 */
/* 	group by lst_grp */
/* 	; */
/* quit; */









/* ----------- *//* ----------- *//* ----------- */
/* ----------- *//* ----------- *//* ----------- */
/* ----------- *//* ----------- *//* ----------- */
/* normalize week-N trending */

proc sql	;
	create table av_pweekN2 as
		select distinct
			lst_grp
			,rent_week
			,min(rent_date) as dow_1st format date9.
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,avg(adj_calppn_) as avg_adj_calppn_ format dollar5.
			,count(*) as N
			,std(adj_calppn_) as std_adj_calppn format=5.
			,compress('('||put(min(adj_calppn_),10.)||','||put(max(adj_calppn_),10.)||')') as minmax_adj_calppn
		from rentdaylevel (where=(lst_grp ne 'Elati'
							and los_weekly=0 and los_monthly=0))
		group by lst_grp, rent_week
		order by lst_grp, rent_week
	;
quit	;
ods proclabel 'av_pweekN - no wk or mos los, no Elati'	;
title 'av_pweekN - no wk or mos los, no Elati';
proc sgplot data=av_pweekN2	;
	vbar rent_week /response=avg_P_adj_calppn_ stat=mean	;
run;


proc sql	;
	create table av_pweekN2 as
		select distinct
			 lst_grp
			,rent_week
			,min(rent_date) as dow_1st format date9.
			,count(*) as N
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,std(P_adj_calppn_) as std_P_adj_calppn format=5.
			,compress('('||put(min(P_adj_calppn_),10.)||','||put(max(P_adj_calppn_),10.)||')') as minmax_P_adj_calppn
		from rentdaylevel (where=(lst_grp ne 'Elati'
							and nights le 5
							and adv_days gt 3
							and P_adj_calppn_ between &p5_beta. and &p95_beta.))
		group by lst_grp, rent_week
		order by lst_grp, rent_week
	;
quit	;
ods proclabel 'av_pweekN - grp by lst-grp and wk, no Elati, 1 to 5 nites, 2sd outliers removed'	;
title 'av_pweekN - grp by lst-grp and wk, no Elati, 1 to 5 nites, 2sd outliers removed'	;
proc sgplot data=av_pweekN2	;
	vbar rent_week /response=avg_P_adj_calppn_ stat=mean	;
run;


proc sql	;
	create table av_pweekN3 as
		select distinct
			 rent_week
			,min(rent_date) as dow_1st format date9.
			,count(*) as N
			,avg(P_adj_calppn_) as avg_P_adj_calppn_ format percent7.
			,std(P_adj_calppn_) as std_P_adj_calppn format=percent7.
			,compress('('||put(min(P_adj_calppn_),percent7.)||','||put(max(P_adj_calppn_),percent7.)||')') as minmax_P_adj_calppn
		from rentdaylevel (where=(lst_grp ne 'Elati'
							and nights le 5
							and adv_days gt 3
							and P_adj_calppn_ between &p5_beta. and &p95_beta.))
		group by rent_week
		order by rent_week
	;
quit	;
ods proclabel 'av_pweekN - all CRVW one group, no Elati, 1 to 5 nites, 2sd outliers removed'	;
title 'av_pweekN - all CRVW one group, no Elati, 1 to 5 nites, 2sd outliers removed'	;
proc sgplot data=av_pweekN3	;
	vbar rent_week /response=avg_P_adj_calppn_	;
run;



/* --- Max Rate Calendaring --- */
/*  */
/* what does the distribution of prices look like per month per listing? */
proc sort data=rentdaylevel out=rdl_sort;
	by lst_grp rent_month	;
run	;
proc univariate data=rdl_sort outtable=rdl_uni noprint;
	by lst_grp rent_month	;
	var ppn	;
run	;
proc print data=rdl_uni (where=(lst_grp ne 'Elati'));
	var lst_grp rent_month _Q1_ _MEDIAN_ _Q3_	;
run;
/* by rent_week */
proc sort data=rentdaylevel out=rdl_weekSort;
	by lst_grp rent_week;
run;
proc univariate data=rdl_weekSort outtable=rdl_weekSort_uni;
	by lst_grp rent_week;
	var ppn P_adj_calppn_	;
run;
proc sort data=rdl_weekSort_uni;
	by _VAR_ lst_grp rent_week;
run;
/* same idea for distribution of P_ppn */
proc univariate data=rdl_sort outtable=rdl_uni noprint;
	by lst_grp rent_month	;
	var P_adj_calppn_	;
run	;
proc print data=rdl_uni (where=(lst_grp ne 'Elati'));
	var lst_grp rent_month _Q1_ _MEDIAN_ _Q3_	;
run;

title "crvw4_future01"	;
title2	" &lst_grp1. average adj_calppn_ = &appn1."	;
footnote  "cal_priceO=max(week_avg_P_adj_calppn,mos_avg_P_adj_calppn)*&appn1.*dow_avg_P_adj_calppn*pric_multi";
footnote2 "cal_price=max(week_avg_adj_calppn,mos_avg_adj_calppn)*dow_avg_P_adj_calppn*pric_multi	";
footnote3 "cal_price_daysouttrend=cal_price*pric_multi	";
options linesize=132;
proc calendar data=crvw4_future01 holidata=trnsp_holi	;
	start cal_date	;
	var calpristr2	;
	mean mos_avg_clean_fee / f=dollar5.	;
	holistart sta;
	holivar act;
run	;
title;title2;footnote;footnote2;footnote3;