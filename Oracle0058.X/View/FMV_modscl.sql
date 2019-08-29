create or replace view fmv_modscl as
select m.mod42_em_addr mod_em_addr,s.scl_em_addr,s.id_scl,s.scl_cle
from modscl m,scl s
where m.scl41_em_addr=s.scl_em_addr;
