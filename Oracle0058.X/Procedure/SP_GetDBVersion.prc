create or replace procedure SP_GetDBVersion(p_Version out varchar2, p_SqlCode out number)
as

begin
       --Get the database version number
       p_Version:='6.1.0.0';
       select nvl(max(version),p_Version) into p_Version from tb_version where state=1;

       p_SqlCode := 0;
end;
/
