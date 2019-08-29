create or replace view v_scl_external_data as
select k.mod42_em_addr,j.scl_cle
from scl j,
     modscl k
where j.scl_em_addr=k.scl41_em_addr
and k.num_modscl=2;
