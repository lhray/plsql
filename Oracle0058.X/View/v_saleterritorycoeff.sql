create or replace view v_saleterritorycoeff as
select id, c1, c2, c3, c4, c5, c6
  from (select g.geo39_em_addr id, g.n0_dvs, d.dvs_cle val
          from geodvs g, dvs d
         where g.dvs38_em_addr = d.dvs_em_addr) pivot(max(val) for n0_dvs in(0 as c1,
                                                                             1 as c2,
                                                                             2 as c3,
                                                                             3 as c4,
                                                                             4 as c5,
                                                                             5 as c6));
