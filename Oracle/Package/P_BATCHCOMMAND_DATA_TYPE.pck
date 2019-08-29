create or replace package P_BATCHCOMMAND_DATA_TYPE is

  -- Author  : YPWANG
  -- Created : 11/16/2012 1:30:59 PM
  -- Purpose : data type and constant for batch command procedure

  SWITCH_a_m_j constant integer := 1;
  SWITCH_aa_mm constant integer := 2;
  SWITCH_am_w  constant integer := 3;
  SWITCH_a_m   constant integer := 4;

  TYPE OptionsRecordType IS record(
    bKeyDis           BOOLEAN := false, --    -key_dis
    strKeyDis         varchar2(32) := '', --    parameter of -key_dis
    bKeyDisDefault    BOOLEAN := false, --    -key_dis_default
    strKeyDisDefault  varchar2(32) := '', --    parameter of -key_dis_default
    bKeyGeo           BOOLEAN := false, --    -key_geo
    strKeyGeo         varchar2(32) := '', --    parameter of -key_geo
    bKeyGeoDefault    BOOLEAN := false, --    -key_geo_default
    strKeyGeoDefault  varchar2(32) := '', --    parameter of -key_geo_default
    bNoDis            BOOLEAN := false, --    -nodis
    bNoGeo            BOOLEAN := false, --    -nogeo
    bR2P              BOOLEAN := false, --    -r2p
    nDateFormat       integer := 0, --    -a_m_j:1, -aa_mm:2, -am_w:3
    bDebut            boolean := false, --    -debut
    bSdate            boolean := false, --    -sdate
    bSel              boolean := false, --    -sel
    strSel            varchar2(200) := '', --    parameter of -sel
    bLib              boolean := false, --  -lib
    nDescription      integer := -1, --    -description
    bPrix             boolean := false, --  -prix
    bSuite            boolean := false, --   -suite
    bTab              boolean := false, --  -tab
    bUM               boolean := false, --   -um
    nUM               integer := 0, --   -um:X
    bVersion          boolean := false, --Version
    nVersion          integer := 0, --Version:X
    bSelCondit        boolean := false, --sel_condit
    strSelCondit      varchar2(400) := '', -- parameter of -sel_condit
    bAttributeInherit boolean := false, --if pimport command specify the option ?attribute_inherit, the dimension?s attribute will inherit from parent level.
    --added begin by zhangxf 20130328 switch :delete null rows
    bNobl             boolean :=false,
    bPar1val          boolean :=false,
    bNd               boolean :=false,
    strNb             varchar2(8) :='',
    --added end
    --added begin by zhanglei 20130412 switch :bills of supply
    bMtotal           boolean :=false,
    bDay              boolean :=false,
    bBomtotal         boolean :=false,
    --added begin by zhuyi
    old_data_drp     integer := 0,
    ajout            integer := 0,
    date_ajout_delai integer := 0,
    --added end
    bPro             boolean :=false,  --parameter of pro
    bGeo             boolean :=false   --parameter of geo
    );

  type OptionsFieldForMatsType is record(
    bTab   boolean := false,
    besp   boolean := false,
    bsbs   boolean := false,
    bsdfm  boolean := false,
    bsdlt  boolean := false,
    bsep   boolean := false,
    bspv   boolean := false,
    bsv    boolean := false,
    bsel   boolean := false,
    strsel varchar2(4000) := '',
    bsel_condit    boolean  := false,
    strsel_condit  varchar2(4000) :='', 
    bOther varchar2(4000) := '');

  type OptionsKeyForMatsType is record(
    bnodis boolean := false,
    bnogeo boolean := false);

end P_BATCHCOMMAND_DATA_TYPE;
/
create or replace package body P_BATCHCOMMAND_DATA_TYPE is

end P_BATCHCOMMAND_DATA_TYPE;
/
