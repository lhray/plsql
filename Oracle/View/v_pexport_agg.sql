create or replace view v_pexport_agg as
select s.sel_em_addr,
       s.sel_cle,
       f.f_cle,
       g.g_cle,
       d.d_cle,
       "C49",
       "C50",
       "C51",
       "C52",
       "C53",
       "C54",
       "C55",
       "C56",
       "C57",
       "C58",
       "C59",
       "C60",
       "C61",
       "C62",
       "C63",
       "C64",
       "C65",
       "C66",
       "C67"
  from sel  s,v_aggnodewithlevel va ,fam f,geo g, dis d,
       (select sel53_em_addr sel_em_addr,
               "C49",
               "C50",
               "C51",
               "C52",
               "C53",
               "C54",
               "C55",
               "C56",
               "C57",
               "C58",
               "C59",
               "C60",
               "C61",
               "C62",
               "C63",
               "C64",
               "C65",
               "C66",
               "C67"
          from (select r.sel53_em_addr,
                       r.numero_crt_sel,
                       cc.val_crt_serie
                  from selcrt r,crtserie cc where cc.crtserie_em_addr = r.crtserie54_em_addr and cc.id_crt_serie = 83
                   union
                  select r.sel_em_addr ,
                  49+c.n0_cdt ,
                  cc.val_crt_serie
                  from cdt c ,sel r, crtserie cc
                  where c.rcd_cdt = 10055
                  and c.operant = 1
                  and c.sel11_em_addr = r.sel_em_addr
                  and c.adr_cdt = cc.crtserie_em_addr
                  ) pivot(max(val_crt_serie) for numero_crt_sel in(49 as c49,
                                                                                     50 as c50,
                                                                                     51 as c51,
                                                                                     52 as c52,
                                                                                     53 as c53,
                                                                                     54 as c54,
                                                                                     55 as c55,
                                                                                     56 as c56,
                                                                                     57 as c57,
                                                                                     58 as c58,
                                                                                     59 as c59,
                                                                                     60 as c60,
                                                                                     61 as c61,
                                                                                     62 as c62,
                                                                                     63 as c63,
                                                                                     64 as c64,
                                                                                     65 as c65,
                                                                                     66 as c66,
                                                                                     67 as c67))) t
 where s.sel_em_addr = t.sel_em_addr(+)
 and s.sel_bud = 71
 and va.aggnodeid = s.sel_em_addr(+)
 and va.PID = f.fam_em_addr(+)
 and va.STID = g.geo_em_addr(+)
 and va.TCID = d.dis_em_addr(+)
 union
 select s.sel_em_addr,
       s.sel_cle,
       null f_cle ,
       null g_cle ,
       null d_cle,
       "C49",
       "C50",
       "C51",
       "C52",
       "C53",
       "C54",
       "C55",
       "C56",
       "C57",
       "C58",
       "C59",
       "C60",
       "C61",
       "C62",
       "C63",
       "C64",
       "C65",
       "C66",
       "C67"
  from sel  s,
       (select sel53_em_addr sel_em_addr,
               "C49",
               "C50",
               "C51",
               "C52",
               "C53",
               "C54",
               "C55",
               "C56",
               "C57",
               "C58",
               "C59",
               "C60",
               "C61",
               "C62",
               "C63",
               "C64",
               "C65",
               "C66",
               "C67"
          from (select r.sel_em_addr sel53_em_addr  ,
                  49+c.n0_cdt numero_crt_sel ,
                  cc.val_crt_serie val_crt_serie
                  from cdt c ,sel r, crtserie cc  where c.sel11_em_addr not in(
                  select distinct c.sel11_em_addr from cdt c where c.rcd_cdt in (10000,10001,10002,20007,20008,20009))
                  and c.rcd_cdt = 10055 and c.operant = 1
                  and c.sel11_em_addr = r.sel_em_addr
                  and c.adr_cdt = cc.crtserie_em_addr
                  ) pivot(max(val_crt_serie) for numero_crt_sel in(49 as c49,
                                                                                     50 as c50,
                                                                                     51 as c51,
                                                                                     52 as c52,
                                                                                     53 as c53,
                                                                                     54 as c54,
                                                                                     55 as c55,
                                                                                     56 as c56,
                                                                                     57 as c57,
                                                                                     58 as c58,
                                                                                     59 as c59,
                                                                                     60 as c60,
                                                                                     61 as c61,
                                                                                     62 as c62,
                                                                                     63 as c63,
                                                                                     64 as c64,
                                                                                     65 as c65,
                                                                                     66 as c66,
                                                                                     67 as c67))) t
 where s.sel_em_addr(+) = t.sel_em_addr
 and s.sel_bud = 71;
