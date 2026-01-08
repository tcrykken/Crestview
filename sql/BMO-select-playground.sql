-- set-up

use 305Analysis;

create table rawBMO_playground (amount decimal(10,2));

insert into rawBMO_playground (amount) select amount from rawBMO;

--

SELECT
    ROW_NUMBER() over (ORDER BY amount) as rn,
    lag(amount) OVER (ORDER BY amount) as lag_amount,
    lead(amount) OVER (ORDER BY amount) as lead_amount,
    amount
from rawBMO_playground;

SELECT
    rn,
    RANK() OVER (ORDER BY amount) AS rank_value,
    DENSE_RANK() OVER (ORDER BY amount) AS denseRank,
    amount
from
    (SELECT
        (@rownum := @rownum + 1) as rn,
        amount
    from rawBMO_playground as amounts
    CROSS JOIN (SELECT @rownum := 0) as r) as numbered_amounts
order by rn;

-- select and clean-up

select * from rawBMO_playground;

drop table if exists rawBMO_playground;