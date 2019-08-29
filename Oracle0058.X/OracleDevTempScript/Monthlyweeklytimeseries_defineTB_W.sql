drop table don_w cascade constraints purge;
drop table prb_W cascade constraints purge;
drop table bud_W cascade constraints purge;

drop sequence seq_DON_W;
drop sequence seq_prb_W;
drop sequence seq_bud_W;

--Week Version Detail Node Time Series-----------------------------------------------------------------------------
create table DON_W
(
  DON_WID       number(19) not null,
  pvtID        INTEGER,
  TSID          INTEGER,
  Version       INTEGER,
  YY            INTEGER,
  T1               number,
  T2               number,
  T3               number,
  T4               number,
  T5               number,
  T6               number,
  T7               number,
  T8               number,
  T9               number,
  T10              number,
  T11              number,
  T12              number,
  T13              number,
  T14              number,
  T15              number,
  T16              number,
  T17              number,
  T18              number,
  T19              number,
  T20              number,
  T21              number,
  T22              number,
  T23              number,
  T24              number,
  T25              number,
  T26              number,
  T27              number,
  T28              number,
  T29              number,
  T30              number,
  T31              number,
  T32              number,
  T33              number,
  T34              number,
  T35              number,
  T36              number,
  T37              number,
  T38              number,
  T39              number,
  T40              number,
  T41              number,
  T42              number,
  T43              number,
  T44              number,
  T45              number,
  T46              number,
  T47              number,
  T48              number,
  T49              number,
  T50              number,
  T51              number,
  T52              number,
  T53              number
)
;
alter table DON_W
  add constraint PK_DON_w primary key (DON_WID);

create index IDX_DON_w_Tpvt_ts_vs on DON_W (pvtID, TSID,Version);

create sequence seq_DON_W
    minvalue 1
    maxvalue 9999999999999999999999999999
    start with 1
    increment by 1
order
cache 10000;

--Week Version Selection and Aggregate Node Time Series-----------------------------------------------------------------------------------
create table prb_W
(
  prb_WID       number(19) not null,
  SelID        INTEGER,
  TSID          INTEGER,
  Version       INTEGER,
  YY            INTEGER,
  T1               number,
  T2               number,
  T3               number,
  T4               number,
  T5               number,
  T6               number,
  T7               number,
  T8               number,
  T9               number,
  T10              number,
  T11              number,
  T12              number,
  T13              number,
  T14              number,
  T15              number,
  T16              number,
  T17              number,
  T18              number,
  T19              number,
  T20              number,
  T21              number,
  T22              number,
  T23              number,
  T24              number,
  T25              number,
  T26              number,
  T27              number,
  T28              number,
  T29              number,
  T30              number,
  T31              number,
  T32              number,
  T33              number,
  T34              number,
  T35              number,
  T36              number,
  T37              number,
  T38              number,
  T39              number,
  T40              number,
  T41              number,
  T42              number,
  T43              number,
  T44              number,
  T45              number,
  T46              number,
  T47              number,
  T48              number,
  T49              number,
  T50              number,
  T51              number,
  T52              number,
  T53              number
)
;
alter table prb_W
  add constraint PK_prb_w primary key (prb_WID);

create index IDX_prb_w_Tsel_ts_vs on prb_W (selID, TSID,Version);

create sequence seq_prb_W
    minvalue 1
    maxvalue 9999999999999999999999999999
    start with 1
    increment by 1
    order
    cache 10000;

--Week Version bdg Time Series-------------------------------------------------------------------------------------------------------
create table bud_W
(
  bud_WID       number(19) not null,
  bdgID         INTEGER,
  TSID          INTEGER,
  Version       INTEGER,
  YY            INTEGER,
  T1               number,
  T2               number,
  T3               number,
  T4               number,
  T5               number,
  T6               number,
  T7               number,
  T8               number,
  T9               number,
  T10              number,
  T11              number,
  T12              number,
  T13              number,
  T14              number,
  T15              number,
  T16              number,
  T17              number,
  T18              number,
  T19              number,
  T20              number,
  T21              number,
  T22              number,
  T23              number,
  T24              number,
  T25              number,
  T26              number,
  T27              number,
  T28              number,
  T29              number,
  T30              number,
  T31              number,
  T32              number,
  T33              number,
  T34              number,
  T35              number,
  T36              number,
  T37              number,
  T38              number,
  T39              number,
  T40              number,
  T41              number,
  T42              number,
  T43              number,
  T44              number,
  T45              number,
  T46              number,
  T47              number,
  T48              number,
  T49              number,
  T50              number,
  T51              number,
  T52              number,
  T53              number
)
;
alter table bud_W
  add constraint PK_bud_w primary key (bud_WID);

create index IDX_bud_w_Tbdg_ts_vs on bud_W (bdgID, TSID,Version);

create sequence seq_bud_W
    minvalue 1
    maxvalue 9999999999999999999999999999
    start with 1
    increment by 1
    order
    cache 10000;
