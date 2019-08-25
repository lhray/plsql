SQL Scripts

FileName                       Description
--------------------------------------------------------------------------------------------
FM610.sql                      the script is for re-creating an initail FM database.It deleted all the old                                            datas first and create new datas then.
fm600_oracle.sql               depleted
fm610_generate_rebuilding_pk_oracle.sql  developers use this script to genereate a script to rebuild index
fm610_oracle.sql               depleted
TB_version.sql                 records of fm database structure changed
Pimport_Tables_1.sql           [pimport]:temporary table .



Procedure
FileName                       Description
--------------------------------------------------------------------------------------------
p_fam_price.prc                return the result of product's price in row
p_fam_unite.prc                the same as above
p_initial datas.prc            [pimport]:initial fam,geo,dis: Pimport_Tables_1.sql
pr_detailnode_timeseries.prc   [pimport]:don,rpd
pr_product_price.prc
pr_product_unit.prc
 

Funcation
FileName                       Description
--------------------------------------------------------------------------------------------
f_convert_hex2dec.fnc          convert a 16bit hex data to dec

Views
FileName                       Description
--------------------------------------------------------------------------------------------
Views.sql                      views.
