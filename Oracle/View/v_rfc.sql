create or replace view v_rfc as
select r.rfc_em_addr,
       chr(decode(r.ident_crt, 70, 80, ident_crt)) dent_crt,
       numero_crt,
       fam7_em_addr,
       geo8_em_addr,
       dis9_em_addr,
       vct10_em_addr,
       fam7_em_addr_ord,
       geo8_em_addr_ord,
       dis9_em_addr_ord,
       vct10_em_addr_ord
  from rfc r;
