create or replace view fmv_aggregatenodewithoperator as
select prv15_em_addr aggregationid,
       sel16_em_addr aggnodeid,
       poperator,
       SToperator,
       TCoperator
  from (select p.prv15_em_addr, p.sel16_em_addr, c.rcd_cdt, c.operant
          from prvsel p
          left join cdt c
            on p.sel16_em_addr = c.sel11_em_addr
           and c.n0_val_cdt = 0
           and c.rcd_cdt between 10000 and 10002) pivot(max(operant) for rcd_cdt in(10000 as
                                                                                    Poperator,
                                                                                    10001 as
                                                                                    SToperator,
                                                                                    10002 as
                                                                                    TCoperator));
