use 305Analysis;

select * from rawBMO_playground limit 2;

-- includes duplicates
select amount from rawBMO_playground where amount > 1000
union all
select amount from rawBMO_playground where amount < -1000
order by amount desc;

-- removes duplicates
select amount from rawBMO_playground where amount > 1000
union
select amount from rawBMO_playground where amount < -1000
order by amount desc;

-- with source tagging
select 'positive' as source, amount from rawBMO_playground where amount > 1000
union all
select 'negative' as source, amount from rawBMO_playground where amount < -1000
order by amount desc;

select description as junk 
from rawBMO_playground 
where FI_TRANSACTION_REFERENCE = '2025120300001'
union all
select posted_date as junk 
from rawBMO_playground 
where FI_TRANSACTION_REFERENCE = '2025120300001';

select description, amount
from rawBMO_playground 
where description like '%zelle%'
union ALL  
select description, amount
from rawBMO_playground 
where description like '%mountain%';

select 
    sum(amount) as total_amount
    ,year
from (
    select description, amount, year(posted_date) as year
    from rawBMO_playground 
    where description like '%zelle%' and description not like '%tim%'
) as zt
group by year
order by year desc;


-- common sql intersect forms
select CREDIT_DEBIT from rawBMO_playground where year(posted_date) = 2025
intersect
select CREDIT_DEBIT from rawBMO_playground where year(posted_date) = 2024;
-- returns all values that appear from both years in a distinct list

select description from rawBMO_playground where year(posted_date) = 2025
intersect
select description from rawBMO_playground where year(posted_date) = 2024;
-- returns all values that appear from both years in a distinct list

