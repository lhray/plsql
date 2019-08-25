create or replace view v_productprices as
select "ID","PRIX_1","PRIX_2","PRIX_3","PRIX_4","PRIX_5","C1","C2","C3","C4","C5"
  from (select f.fam_em_addr id,
       m.num_trf,
       r.trf_cle       val,
       f.prix_1,
       f.prix_2,
       f.prix_3,
       f.prix_4,
       f.prix_5
  from fam f
  left join famtrf m
    on f.fam_em_addr = m.fam33_em_addr
  left join trf r on 
   m.trf34_em_addr = r.trf_em_addr) pivot(max(val) for num_trf in(0 as c1,
                                                                              1 as c2,
                                                                              2 as c3,
                                                                              3 as c4,
                                                                              4 as c5));
