--FORM¡¡add field
truncate table FORM;
drop table CONTENU_ETAT  CASCADE constraints purge;

--IST¡¡add field
truncate table IST;
drop table IST_XMLDATA  CASCADE constraints purge;

--User_Data¡¡add field
truncate table User_;
truncate table User_Data;
drop table USER_XMLDATA CASCADE constraints purge;

--PROFIL_DATA add filed
truncate table PROFIL;
truncate table PROFIL_DATA;
drop table PROFIL_XMLDATA  CASCADE constraints purge;

--Menu_data add field 
truncate table PROFIL;
truncate table MENU_data;

drop table Menu_xmldata CASCADE constraints purge;