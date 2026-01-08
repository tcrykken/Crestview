create table if not exists 305Analysis.rawBMO_playground like 305Analysis.rawBMO;

insert into 305Analysis.rawBMO_playground select * from 305Analysis.rawBMO;

alter table 305Analysis.rawBMO_playground
add column status varchar(50);

alter table 305Analysis.rawBMO_playground
add column (dropThis decimal(10,2), category varchar(100), month varchar(20), year integer);

alter table 305Analysis.rawBMO_playground
modify column month date;

-- alter table 305Analysis.rawBMO_playground
-- alter column year date;

alter table 305Analysis.rawBMO_playground
drop column dropThis;

alter table 305Analysis.rawBMO_playground
rename column ORIGINAL_AMOUNT to empty_original_amount;

alter table 305Analysis.rawBMO_playground
add constraint pk_txn_ref_num primary key (TRANSACTION_REFERENCE_NUMBER);
-- Duplicate entry '' for key 'rawbmo_playground.PRIMARY'

alter table 305Analysis.rawBMO_playground
add constraint chk_amount check (amount > -10000);

alter table 305Analysis.rawBMO_playground
add constraint fk_trxn_id foreign key (TRANSACTION_REFERENCE_NUMBER) references othertable (othertabletransactionreferencenumber);

alter table 305Analysis.rawBMO_playground add constraint unique_txn_id unique (TRANSACTION_REFERENCE_NUMBER);

show create table 305Analysis.rawBMO_playground;

alter table 305Analysis.rawBMO_playground
drop constraint if exists pk_txn_ref_num;

ALTER TABLE 305Analysis.rawBMO_playground
DROP CONSTRAINT chk_amount;

drop table if exists 305Analysis.rawBMO_playground;