alter table continuation_node add constraint pk_continuation_node primary key (nodeid,type);

--wfq--
alter table DON_W
  add constraint PK_DON_w primary key (DON_WID);

create index IDX_DON_W_pvt_ts_vs on DON_W (pvtID, TSID,Version);

create sequence seq_DON_W
    minvalue 1
    maxvalue 9999999999999999999999999999
    start with 1
    increment by 1
order;

alter table prb_W
  add constraint PK_prb_w primary key (prb_WID);

create index IDX_prb_W_sel_ts_vs on prb_W (selID, TSID,Version);

create sequence seq_prb_W
    minvalue 1
    maxvalue 9999999999999999999999999999
    start with 1
    increment by 1
    order;

alter table bud_W
  add constraint PK_bud_w primary key (bud_WID);

create index IDX_bud_W_bdg_ts_vs on bud_W (bdgID, TSID,Version);

create sequence seq_bud_W
    minvalue 1
    maxvalue 9999999999999999999999999999
    start with 1
    increment by 1
    order;
