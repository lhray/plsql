create or replace view v_aggnodewithlevel as
select prv15_em_addr aggregationid,--aggregation rule id
       sel_em_addr aggnodeid,--aggregate node ID
       sel_cle     name,--aggregate node name
       sel_desc    description,--description of aggregate node
       p           PID,--id of product
       ST          STID,--id of sale territory
       TC          TCID--id of trade channel
  from (select p.prv15_em_addr,s.sel_em_addr, s.sel_cle, s.sel_desc, c.rcd_cdt, c.adr_cdt
          from prvsel p, sel s, cdt c
         where p.sel16_em_addr = s.sel_em_addr
           and s.sel_em_addr = c.sel11_em_addr
           and s.sel_bud = 71
           and c.operant <> 0
           and c.rcd_cdt between 10000 and 10002) pivot(max(adr_cdt) for rcd_cdt in(10000 as P,
                                                                                                       10001 as ST,
                                                                                                       10002 as TC));
comment on column V_AGGNODEWITHLEVEL.AGGREGATIONID is 'aggregate rule ID';
comment on column V_AGGNODEWITHLEVEL.AGGNODEID is 'aggregate node ID';
comment on column V_AGGNODEWITHLEVEL.NAME is 'aggregate node name';
comment on column V_AGGNODEWITHLEVEL.DESCRIPTION is 'description of aggregate node';
comment on column V_AGGNODEWITHLEVEL.PID is 'id of product';
comment on column V_AGGNODEWITHLEVEL.STID is 'id of sale territory';
comment on column V_AGGNODEWITHLEVEL.TCID is 'id of trade channe';
