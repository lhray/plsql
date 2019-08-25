CREATE OR REPLACE VIEW FMV_GETSELECTIONOBJECT AS
select c.rcd_cdt,c.sel11_em_addr,c.n0_cdt+49 numero_crt,
max(c.adr_cdt) ma ,
min(c.adr_cdt) mi
from cdt c,sel s where c.sel11_em_addr !=0 and c.operant !=2
and c.sel11_em_addr = s.sel_em_addr and s.sel_bud = 0
group by c.rcd_cdt,c.sel11_em_addr,c.n0_cdt+49;
