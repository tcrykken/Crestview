select substr(string,1,5) from 305Analysis.deltempraw
union ALL
select concat('ttttt' ,substr(string,6)) from 305Analysis.deltempraw;


select * from deltempraw limit 1;