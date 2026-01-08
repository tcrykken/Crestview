-- setup
use 305Analysis;

create table rawBMO_playground like rawBMO;

insert into rawBMO_playground select * from rawBMO;

-- casting

select cast(posted_date as char) as string_date from rawBMO_playground;

alter table rawBMO_playground
add column string_date char;

alter table rawBMO_playground
modify column string_date varchar(255);

select * from rawBMO_playground;

update rawBMO_playground
set string_date = cast(posted_date as char);

select amount from rawBMO_playground;
select cast(amount as signed integer) as signed_amount from rawBMO_playground;

select string_date, concat_ws(' ', string_date, '12:00:00') as this, cast(concat_ws(' ', string_date, '12:00:00') as datetime) as spoof_dt from rawBMO_playground;

select 
    (select UUID()) as UUID,
    string_date, 
    current_date as now_d, 
    current_time as now_t, 
    concat_ws(' ', 
        cast(current_date as CHAR), 
        cast(current_time as CHAR)) as now_dt 
from rawBMO_playground;

SELECT
    amount,
    cast(amount as signed) as signed_amount,
    CASE
        WHEN amount >= 0 THEN cast(amount as decimal(10,2))
        WHEN amount < 0 THEN cast(abs(amount) as decimal(10,2))
    END as absAmount,
    CASE
        WHEN amount >= 0 THEN cast(amount as decimal(10,2))
        ELSE 0
    END as credit,
    CASE
        WHEN amount <0 then cast(abs(amount) as decimal(10,2))
        else 0
    end as debit
from rawBMO_playground;

-- clean-up
drop table if exists rawBMO_playground;