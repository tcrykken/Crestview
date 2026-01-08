-- must be run on MySQL shell on command line not Cursor

-- shell> brew services start mysql
-- shell> mysql --local-infile -u root -p

use 305Analysis;

drop table if exists 305Analysis.rawBMO;
create table rawBMO (
    POSTED_DATE date,
    DESCRIPTION varchar(255),
    AMOUNT decimal(10,2),
    CURRENCY varchar(255),
    TRANSACTION_REFERENCE_NUMBER varchar(255),
    FI_TRANSACTION_REFERENCE varchar(255),
    TYPE varchar(255),
    CREDIT_DEBIT varchar(255),
    ORIGINAL_AMOUNT varchar(255));

load data local infile '/Users/a2338-home/Documents/Crestview/305Analysis/data/Raw/BMO_transactions_202309_202512.csv' 
replace into table 305Analysis.rawBMO 
fields terminated by ','
lines terminated by '\n'
ignore 1 lines
(@POSTED_DATE, DESCRIPTION, AMOUNT, CURRENCY, TRANSACTION_REFERENCE_NUMBER, FI_TRANSACTION_REFERENCE, TYPE, CREDIT_DEBIT, @ORIGINAL_AMOUNT)
set POSTED_DATE = str_to_date(@POSTED_DATE, '%m/%d/%Y');
