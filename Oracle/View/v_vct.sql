create or replace view v_vct as
select v.vct_em_addr,
        v.vct_field,
        chr(v.id_crt) id_crt,
        v.num_crt,
        v.val,
        v.lib_crt,
        v.lib_crt_court,
        v.user_create_vct,
        v.date_create_vct,
        v.user_modify_vct,
        v.date_modify_vct
   from vct v;
