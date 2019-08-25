create or replace view fmv_productUnite_inrow as
select fam_em_addr, unite_1 unite, 1 NO
  from fam
 where unite_1 is not null
union all
select fam_em_addr, unite_2 unite, 2 NO
  from fam
 where unite_2 is not null
union all
select fam_em_addr, unite_3 unite, 3 NO
  from fam
 where unite_3 is not null
union all
select fam_em_addr, unite_4 unite, 4 NO
  from fam
 where unite_4 is not null
union all
select fam_em_addr, unite_5 unite, 5 NO
  from fam
 where unite_5 is not null
union all
select fam_em_addr, unite_6 unite, 6 NO
  from fam
 where unite_6 is not null
union all
select fam_em_addr, unite_7 unite, 7 NO
  from fam
 where unite_7 is not null
union all
select fam_em_addr, unite_8 unite, 8 NO
  from fam
 where unite_8 is not null
union all
select fam_em_addr, unite_9 unite, 9 NO
  from fam
 where unite_9 is not null
union all
select fam_em_addr, unite_10 unite, 10 NO
  from fam
 where unite_10 is not null;
