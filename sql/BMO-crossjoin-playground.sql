-- set-up

use 305Analysis;

create table rawBMO_playground (amount decimal(10,2));

insert into rawBMO_playground (amount) select amount from rawBMO;
