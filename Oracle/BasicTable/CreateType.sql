create or replace type FMT_type is table of varchar2(100);
/
create or replace type FMT_obj_nodeid force is object
(
  id number
);
/
create or replace type FMT_nest_tab_nodeid is table  of FMT_obj_nodeid;
/
create or replace type FMT_node_timeseries is table of varchar2(100);
/
create or replace type FMT_NodeAttriValue force is object (NodeID number, AttriValueID number);
/
create or replace type FMT_NodeAttriValue_Array is table of FMT_NodeAttriValue;
/
create or replace type FMT_tnlists is table of number;
/
create or replace type FMT_NODETS force as object
(
  AGGNODE VARCHAR2(200),
  product VARCHAR2(60),
  sales   VARCHAR2(60),
  trade   VARCHAR2(60),
  yy      VARCHAR2(4),
  mm      VARCHAR2(2),
  t_data  number
);
/
create or replace type FMT_NODETIMESERIES is table of FMT_NODETS;
/
