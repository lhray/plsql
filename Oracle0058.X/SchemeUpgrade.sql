--------------------
----This script is use to create new objects in the current schema
----Check the object  is exist,if not create it.
--------------------
prompt  CREATE TABLE  MOD_BLOB_DATA
declare
  v_isExisted number := 0;
  v_sqltext   clob;
  v_tablename varchar2(30) := 'MOD_BLOB_DATA';
begin
  select count(1)
    into v_isExisted
    from user_tables t
   where t.TABLE_NAME = v_tablename;

  if v_isExisted = 1 then
    return;
  end if;
  v_sqltext := 'CREATE TABLE ' || v_tablename || '
(
  MOD_EM_ADDR                  NUMBER(19)       NOT NULL,
  BDG_EM_ADDR                  NUMBER,
  TYPE_ID                      INTEGER,
  ADAPT_ALPHA                  INTEGER,
  ALPHA_INIT                   NUMBER,
  FORCE_SAIS                   INTEGER,
  FILTRAGE                     INTEGER,
  NBPERIODE                    INTEGER,
  OBJECTIF                     INTEGER,
  VALOBJECTIF                  NUMBER,
  FORCE_SAIS_J                 INTEGER,
  MOYENNE                      NUMBER,
  TENDANCE                     NUMBER,
  AWS                          NUMBER,
  TOTMOINS12                   NUMBER,
  TOTPLUS12                    NUMBER,
  TOTANPREC                    NUMBER,
  TOTANCOURS                   NUMBER,
  PREVANCOURS                  NUMBER,
  ERR1                         NUMBER,
  ERR2                         NUMBER,
  MAD                          NUMBER,
  PREVANSUIV                   NUMBER,
  TYPE_PARAM                   INTEGER,
  AVEC_AS                      INTEGER,
  NF                           NUMBER,
  RESTE_A_FAIRE                NUMBER,
  TOTPLUS12_OBJ                NUMBER,
  PREVANCOURS_OBJ              NUMBER,
  PREVANSUIV_OBJ               NUMBER,
  RESTE_A_FAIRE_OBJ            NUMBER,
  RATIO_12                     NUMBER,
  RATIO_12_OBJ                 NUMBER,
  RATIO_COUR_PREC              NUMBER,
  RATIO_COUR_PREC_OBJ          NUMBER,
  RATIO_SUIV_COUR              NUMBER,
  RATIO_SUIV_COUR_OBJ          NUMBER,
  RATIO_REAL                   NUMBER,
  RATIO_REAL_OBJ               NUMBER,
  RATIO_A_FAIRE                NUMBER,
  RATIO_A_FAIRE_OBJ            NUMBER,
  AVEC_OBJ                     INTEGER,
  MAJ_BATCH                    INTEGER,
  ERR_PRV_6M                   NUMBER,
  NB_HISTO                     INTEGER,
  TAUX_SUITE_DE                NUMBER,
  DECALAGE                     INTEGER,
  HAUTEUR_REAPROVIS            NUMBER,
  TYPE_OBJECTIF                INTEGER,
  SERIE                        INTEGER,
  HORIZON                      INTEGER,
  TAUX_EXPLIT                  NUMBER,
  ECART_PRV_DELAY              NUMBER,
  FORCE_SAIS_DECADE            INTEGER,
  NBPERIODE_JOUR               INTEGER,
  OBJECTIF2                    INTEGER,
  VALOBJECTIF2                 NUMBER,
  UNITE_OBJECTIF2              INTEGER,
  HORIZON_AFFICHAGE            INTEGER,
  RATIO_PRORATA                INTEGER,
  COEF_SAIS1                   NUMBER,
  COEF_SAIS2                   NUMBER,
  COEF_SAIS3                   NUMBER,
  COEF_SAIS4                   NUMBER,
  COEF_SAIS5                   NUMBER,
  COEF_SAIS6                   NUMBER,
  COEF_SAIS7                   NUMBER,
  COEF_SAIS8                   NUMBER,
  COEF_SAIS9                   NUMBER,
  COEF_SAIS10                  NUMBER,
  COEF_SAIS11                  NUMBER,
  COEF_SAIS12                  NUMBER,
  COEF_SAIS13                  NUMBER,
  COEF_SAIS14                  NUMBER,
  COEF_SAIS15                  NUMBER,
  COEF_SAIS16                  NUMBER,
  COEF_SAIS17                  NUMBER,
  COEF_SAIS18                  NUMBER,
  COEF_SAIS19                  NUMBER,
  COEF_SAIS20                  NUMBER,
  COEF_SAIS21                  NUMBER,
  COEF_SAIS22                  NUMBER,
  COEF_SAIS23                  NUMBER,
  COEF_SAIS24                  NUMBER,
  COEF_SAIS25                  NUMBER,
  COEF_SAIS26                  NUMBER,
  COEF_SAIS27                  NUMBER,
  COEF_SAIS28                  NUMBER,
  COEF_SAIS29                  NUMBER,
  COEF_SAIS30                  NUMBER,
  COEF_SAIS31                  NUMBER,
  COEF_SAIS32                  NUMBER,
  COEF_SAIS33                  NUMBER,
  COEF_SAIS34                  NUMBER,
  COEF_SAIS35                  NUMBER,
  COEF_SAIS36                  NUMBER,
  COEF_SAIS37                  NUMBER,
  COEF_SAIS38                  NUMBER,
  COEF_SAIS39                  NUMBER,
  COEF_SAIS40                  NUMBER,
  COEF_SAIS41                  NUMBER,
  COEF_SAIS42                  NUMBER,
  COEF_SAIS43                  NUMBER,
  COEF_SAIS44                  NUMBER,
  COEF_SAIS45                  NUMBER,
  COEF_SAIS46                  NUMBER,
  COEF_SAIS47                  NUMBER,
  COEF_SAIS48                  NUMBER,
  COEF_SAIS49                  NUMBER,
  COEF_SAIS50                  NUMBER,
  COEF_SAIS51                  NUMBER,
  COEF_SAIS52                  NUMBER,
  COEF_SAIS_JOUR1              NUMBER,
  COEF_SAIS_JOUR2              NUMBER,
  COEF_SAIS_JOUR3              NUMBER,
  COEF_SAIS_JOUR4              NUMBER,
  COEF_SAIS_JOUR5              NUMBER,
  COEF_SAIS_JOUR6              NUMBER,
  COEF_SAIS_JOUR7              NUMBER,
  UNUSED_ADDR                  NUMBER,
  DATE_PREV_ANNEE              INTEGER,
  DATE_PREV_PERIODE            INTEGER,
  DEBUT_UTIL_SAISON_ANNEE      INTEGER,
  DEBUT_UTIL_SAISON_PERIODE    INTEGER,
  FIN_UTIL_SAISON_ANNEE        INTEGER,
  FIN_UTIL_SAISON_PERIODE      INTEGER,
  DATE_FIN_PREV_ANNEE          INTEGER,
  DATE_FIN_PREV_PERIODE        INTEGER,
  DATE_DEB_PREV_ANNEE          INTEGER,
  DATE_DEB_PREV_PERIODE        INTEGER,
  DEBUT_UTIL_ANNEE             INTEGER,
  DEBUT_UTIL_PERIODE           INTEGER,
  DEBUT_HISTO_ANNEE            INTEGER,
  DEBUT_HISTO_PERIODE          INTEGER,
  DATE_FIN_HISTO_ANNEE         INTEGER,
  DATE_FIN_HISTO_PERIODE       INTEGER,
  DATE_DEB_OBJ_ANNEE           INTEGER,
  DATE_DEB_OBJ_PERIODE         INTEGER,
  DATE_FIN_OBJ_ANNEE           INTEGER,
  DATE_FIN_OBJ_PERIODE         INTEGER,
  DEBUT_UTIL_SAISON_J_ANNEE    INTEGER,
  DEBUT_UTIL_SAISON_J_PERIODE  INTEGER,
  FIN_UTIL_SAISON_J_ANNEE      INTEGER,
  FIN_UTIL_SAISON_J_PERIODE    INTEGER,
  DATE_DEBUT_SUITE_ANNEE       INTEGER,
  DATE_DEBUT_SUITE_PERIODE     INTEGER,
  DATE_FIN_SUITE_ANNEE         INTEGER,
  DATE_FIN_SUITE_PERIODE       INTEGER,
  DATE_CHOW_ANNEE              INTEGER,
  DATE_CHOW_PERIODE            INTEGER,
  DATE_PREV_JOUR_ANNEE         INTEGER,
  DATE_PREV_JOUR_PERIODE       INTEGER,
  NIVEAU                       NUMBER,
  PENTE                        NUMBER,
  RESULTAT_CUSUM               INTEGER,
  HORIZONFUTUR                 INTEGER,
  HORIZONPASSE                 INTEGER,
  CHOIXFONCTION                INTEGER,
  SAISONNALITE                 INTEGER,
  GESTIONDESBORDS              INTEGER,
  UNUSED_A                     INTEGER,
  MAX_NBPERIODE_SAIS           INTEGER,
  MAX_NBPERIODE_SAIS_JOUR      INTEGER,
  CALCULAUTOJF                 INTEGER,
  INDICE_PREV                  NUMBER,
  BESTFIT                      INTEGER,
  DUREEBESTFIT                 INTEGER,
  JOUR_DATE_DEB_PREV           INTEGER,
  JOUR_DATE_FIN_PREV           INTEGER,
  JOUR_DATE_DEB_OBJ            INTEGER,
  JOUR_DATE_FIN_OBJ            INTEGER,
  SZBESTFITRULENAME            CHAR(31 BYTE),
  SZBESTFITRULEDESC            CHAR(61 BYTE),
  DLASTMODIFIEDTIME            NUMBER,
  COEF_CORREL_R2               NUMBER
)';
  execute immediate v_sqltext;
exception
  when others then
    dbms_output.put_line('Create table ' || v_tablename || ' eroor ' ||
                         sqlcode);
end;
/

prompt CREATE TABLE  AGGREGATENODE_FULLID
declare

  v_isExisted number := 0;
  v_sqltext   clob;
  v_tablename varchar2(30) := 'AGGREGATENODE_FULLID';
begin
  select count(1)
    into v_isExisted
    from user_tables t
   where t.TABLE_NAME = v_tablename;
  if v_isExisted = 1 then
    return;
  end if;
  v_sqltext := 'CREATE TABLE ' || v_tablename || '
                (
                  AGGREGATIONID   NUMBER,
                  AGGREGATENODEID NUMBER,
                  AGGREGATEFULLID VARCHAR2(400),
                  NAME            VARCHAR2(186),
                  DESCRIPTIONS    VARCHAR2(120)
                )';
  execute immediate v_sqltext;
  
  v_sqltext := 'create index idx_aggruleid_ids_nodeid on aggregatenode_fullid(aggregationid,aggregatefullid,aggregatenodeid)';
  execute immediate v_sqltext;
exception
  when others then
    dbms_output.put_line('Create table ' || v_tablename || ' eroor ' ||
                         sqlcode);
end;
/




prompt CREATE TABLE  PRVSELPVT
declare
  v_isExisted number := 0;
  v_sqltext   clob;
  v_tablename varchar2(30) := 'PRVSELPVT';
begin
  select count(1)
    into v_isExisted
    from user_tables t
   where t.TABLE_NAME = v_tablename;
  if v_isExisted = 1 then
    return;
  end if;
  v_sqltext := 'CREATE TABLE ' || v_tablename || '              
              (
                PRVID INTEGER,
                SELID INTEGER,
                PVTID INTEGER
              )';
  execute immediate v_sqltext;

  v_sqltext := 'create index idx_prvselpvt_prvid on prvselpvt(prvid)';
  execute immediate v_sqltext;
exception
  when others then
    dbms_output.put_line('Create table ' || v_tablename || ' eroor ' ||
                         sqlcode);
end;
/

prompt ALTER  TABLE  LOG_OPERATION
declare
  v_isExisted number := 0;
  v_sqltext   clob;
  v_tablename varchar2(30) := 'LOG_OPERATION';
  v_table_col varchar2(30) := 'SQLTEXT';
begin
  select count(1)
    into v_isExisted
    from user_tab_cols
   where table_name = v_tablename
     AND COLUMN_NAME = v_table_col;
  if v_isExisted = 1 then
    return;
  end if;
  v_sqltext := 'alter  TABLE ' || v_tablename || ' add  ' || v_table_col ||
               ' clob';
  execute immediate v_sqltext;
exception
  when others then
    dbms_output.put_line('alter table ' || v_tablename || ' add column ' ||
                         v_table_col || ' eroor ' || sqlcode);
end;
/

