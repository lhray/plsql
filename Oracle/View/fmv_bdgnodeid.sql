create or replace view fmv_bdgnodeid as
select p.pvt_em_addr nodeid,
       b.bdg_em_addr bdgid,
       b.b_cle       key,
       b.bdg_desc    description,
       0             nodetype
  from pvt p, bdg b
 where b.id_bdg = 80
   and b.b_cle = p.pvt_cle
union all
select s.sel_em_addr nodeid,
       b.bdg_em_addr bdgid,
       b.b_cle       key,
       b.bdg_desc    description,
       1             nodetype
  from sel s, bdg b
 where b.id_bdg = 71
   and b.b_cle = s.sel_cle
   and s.sel_bud = 71;
