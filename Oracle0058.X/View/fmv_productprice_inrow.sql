create or replace view fmv_productprice_inrow as
select fam_em_addr, prix_1 price, 1 NO
  from fam
 where prix_1 is not null
union all
select fam_em_addr, prix_2 price, 2 NO
  from fam
 where prix_2 is not null
union all
select fam_em_addr, prix_3 price, 3 NO
  from fam
 where prix_3 is not null
union all
select fam_em_addr, prix_4 price, 4 NO
  from fam
 where prix_4 is not null
union all
select fam_em_addr, prix_5 price, 5 NO
  from fam
 where prix_5 is not null
union all
select fam_em_addr, prix_6 price, 6 NO
  from fam
 where prix_6 is not null
union all
select fam_em_addr, prix_7 price, 7 NO
  from fam
 where prix_7 is not null
union all
select fam_em_addr, prix_8 price, 8 NO
  from fam
 where prix_8 is not null
union all
select fam_em_addr, prix_9 price, 9 NO
  from fam
 where prix_9 is not null
union all
select fam_em_addr, prix_10 price, 10 NO
  from fam
 where prix_10 is not null;
