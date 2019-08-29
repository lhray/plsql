create or replace procedure SP_GetFMDBUpgradeVersion(p_Version out varchar2)
as
begin
       --Get the database code version number
       p_Version:='7.0.0058.27';
end;
/