drop table if EXISTS 305Analysis.rawBMO_playground;

create table if not exists 305Analysis.rawBMO_playground like 305Analysis.rawBMO;

insert into 305Analysis.rawBMO_playground select * from 305Analysis.rawBMO;

select * from 305Analysis.rawBMO_playground where FI_TRANSACTION_REFERENCE = '' or FI_TRANSACTION_REFERENCE is null;

update 305Analysis.rawBMO_playground set TRANSACTION_REFERENCE_NUMBER = UUID() where TRANSACTION_REFERENCE_NUMBER = '' or TRANSACTION_REFERENCE_NUMBER is null;

select * from 305Analysis.rawBMO_playground;

alter table 305Analysis.rawBMO_playground alter column posted_date set default (current_date);

insert into 305Analysis.rawBMO_playground (posted_date, description, amount, currency) values ('2025-12-08','this is inserted txn', 123.45, 'USD' );

insert into 305Analysis.rawBMO_playground (posted_date, description, amount, currency) values ('2025-12-08', 'multi insert 1', 234.56, 'USD'), ('2025-12-08', 'multi insert 2', 345.67, 'USD'), ('2025-12-08', 'multi insert 3', 456.78, 'USD');

insert into 305Analysis.rawBMO_playground 
select * from 305Analysis.rawBMO_playground where posted_date = '2025-12-08';

alter table 305Analysis.rawBMO_playground 
alter column currency set default 'UNK';

insert into 305Analysis.rawBMO_playground (posted_date, description, amount, currency) 
values ('2025-12-08', 'default', 567.89, DEFAULT)

insert into 305Analysis.rawBMO_playground (description) 
values (NULL), (NULL)

use 305Analysis;

insert into rawBMO_playground (posted_date, description) 
values (current_date, lower('LOWER_THIS_CAPS_EXP'));

insert into rawBMO_playground (posted_date) 
values (current_date);

select * from rawBMO_playground where FI_TRANSACTION_REFERENCE is null;

update rawBMO_playground
set FI_TRANSACTION_REFERENCE = UUID()
where FI_TRANSACTION_REFERENCE is null;

alter table rawBMO_playground 
add constraint pk_fi_trxn_ref_num primary key (FI_TRANSACTION_REFERENCE);

insert ignore into rawBMO_playground (description, FI_TRANSACTION_REFERENCE) values ('this insert should be ignored on duplicate key violation', '768e82c0-d464-11f0-8842-68ae32ee1395');

--  upsert
insert into rawBMO_playground (description, FI_TRANSACTION_REFERENCE) values ('multi insert 1','768e818a-d464-11f0-8842-68ae32ee1395') on duplicate key update description = upper(description);

-- check statements

select * from 305Analysis.rawBMO_playground order by posted_date;

select * from rawBMO_playground where date(POSTED_DATE) = '2025-12-08';

-- clean up
drop table if EXISTS rawBMO_playground;