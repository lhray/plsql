create or replace package p_Constant is

  -- Created By JYLiu on 7/12/2012 define constant

  -- Public constant declarations

  --It's easily confused.in some cases ,80,70,10000 is the same mean,and so on...
  -- define in cdt.rcd_cdt
  PRODUCT        constant number := 10000; --product key
  SALE_TERRITORY constant number := 10001; --sale territory key
  TRADE_CHANNEL  constant number := 10002; --trade channel key
  BoM            constant number := 10039; --BoM
  BoS            constant number := 10005; --BoS
  ProcessOfData  constant number := 10047; --process item in data form
  DETAIL_NODE    constant number := 10055; --detail node attributes
  v_ProductAttr  constant number := 20007; --product's attributes
  v_STAttr       constant number := 20008; --sale territory's attributes
  v_TCAttr       constant number := 20009; --trade channel's attributes

  -- define in vct.id_crt
  v_ProductData    constant number := 80; --product data
  v_STData         constant number := 71; --sale territory data
  v_TCData         constant number := 68; --trade channel data
  v_DetailNodeData constant number := 83; --time series

  --nct.num_ct and vct.crt should be BaseNumberOfAttr + Attribute Index
  BaseNumberOfAttr constant number := 48;
  NumberForUOM     constant number := 69;
  NumberForVU      constant number := 68;

  --define in rfc.ident_crt
  v_RFC_P  constant number := 70; -- product data
  v_RFC_ST constant number := 71; --sale territory data
  v_RFC_TC constant number := 68; --trade channel data

  --error code defined
  e_oraerr constant number := -20004; --ora error

  --Time series version
  Monthly constant number := 1;
  Weekly  constant number := 2;
  Daily   constant number := 3;

  --Node type
  Detail_Node_Type    constant number := 1;
  Aggregate_Node_Type constant number := 2;

  --is bdg type
  IsBDG    constant number := 1;
  IsNotBDG constant number := 0;

  --define about bdg.id_bdg
  ID_DetailNode      constant number := 80;
  ID_AggregationNode constant number := 71;

  --define about sel.sel_bud
  ID_SEL_AggregationNode constant number := 71;
  ID_SEL_Selection       constant number := 0;

  --blank value
  BlankValue constant number := 1E-20;

  --fam.id_fam
  product_group_id constant number := 70;
  product_id       constant number := 80;

  --nmc.nmc_field
  NMCID_FAM_CONTINUE constant number := 83;
end p_Constant;
/
create or replace package body p_Constant is

begin
  null;
end p_Constant;
/
