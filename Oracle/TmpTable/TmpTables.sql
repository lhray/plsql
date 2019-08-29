--drop table tmp_product purge;
--drop table tmp_sales_territory purge;
--drop table tmp_trade_channel purge;
--drop table tmp_Detail_Node purge;
--drop table tmp_Agg_Node purge;
--drop table tmp_cdt purge;
--drop table tmp_AggNodeRule  purge;
--session level
create global temporary table tmp_product(ProductID number) on commit preserve rows;
create global temporary table tmp_sales_territory(territoryid number) on commit preserve rows;
create global temporary table tmp_trade_channel(channelid number) on commit preserve rows;
create global temporary table tmp_Detail_Node(DetailNodeID number) on commit preserve rows;
create global temporary table tmp_Agg_Node(AggNodeID number) on commit preserve rows;
create global temporary table tmp_cdt(tabid number,attrordno number,ope number,val_idx number,addr number) on commit preserve rows;
--table :when a detail node meet all of the rules of an aggregation .record it in this table
create global temporary table tmp_AggNodeRule(AttributeType number,AttributeNo number,AttributeValID number) on commit preserve rows;
comment on column tmp_AggNodeRule.ATTRIBUTENO is 'the index of the attributes from 0 to 18';

