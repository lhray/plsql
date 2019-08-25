create or replace package FMP_ClEARTBMID is

  -- Author  : LZHANG
  -- Created : 5/9/2013 11:30:40 AM
  -- Purpose :

  -- Public type declarations
  procedure FMSP_CLEARTBMID(pIn_vFmuser   in varchar2,
                            pIn_nInterval in number);

end FMP_ClEARTBMID;
/
create or replace package body FMP_ClEARTBMID is
  procedure FMSP_GetSql(pIn_vFmuser   in varchar2,
                        pIn_nInterval in number,
                        pOut_sCursor  out sys_refcursor) IS
  Begin
    declare
      vUser varchar2(128);
      vSql  varchar2(4000);
    begin
      select user into vUser from dual;
    
      if vUser = 'SYS' then
        vSql := 'select OWNER, OBJECT_NAME, created
                    from dba_objects
                 where object_type = ''TABLE''
                    and object_name like ''TBMID%''
                    and created <
                    trunc(sysdate + 1) - 1 / 24 * ' ||
                to_char(pIn_nInterval);
        if pIn_vFmuser is not null then
          vSql := vSql || ' and  owner = ''' || upper(pIn_vFmuser) || '''';
        end if;
        open pOut_sCursor for vSql;
      else
        open pOut_sCursor for
          select vUser OWNER, OBJECT_NAME, created
            from user_objects
           where object_type = 'TABLE'
             and object_name like 'TBMID%'
             and created < sysdate - 1 / 24 * pIn_nInterval;
      end if;
    end;
  End FMSP_GetSql;

  procedure FMSP_Exec(pIn_vSql in varchar2, pOut_nSqlCode out number) IS
  Begin
    begin
      execute immediate pIn_vSql;
      pOut_nSqlCode := 0;
    exception
      when others then
        Fmp_Log.LOGERROR(pIn_vSql);
        pOut_nSqlCode := sqlCode;
    end;
  End FMSP_Exec;

  procedure FMSP_CLEARTBMID(pIn_vFmuser   in varchar2,
                            pIn_nInterval in number) IS
    --*****************************************************************
    -- Description: clear all useless temp table which created time less than or equal to today-interval for all user or special user
    -- Parameters:
    -- pIn_vFmuser if null means all user else is specify user
    -- pIn_nInterval unit is hour
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        9-May-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      type USELESSOBJECTTYPE is record(
        OWNER       VARCHAR2(30),
        OBJECT_NAME varchar2(128),
        created     date);
      userLessObject USELESSOBJECTTYPE;
      sCursor        sys_refcursor;
      vSql           varchar2(4000);
      nSqlCode       number;
    begin
      Fmp_Log.FMP_SetValue(pIn_vFmuser);
      Fmp_Log.FMP_SetValue(pIn_nInterval);
      Fmp_Log.LOGBEGIN;
      FMSP_GetSql(pIn_vFmuser   => pIn_vFmuser,
                  pIn_nInterval => pIn_nInterval,
                  pOut_sCursor  => sCursor);
      FMP_LOG.FMP_DEL(pIn_dDate => (sysdate - 1 / 24 * pIn_nInterval));
      loop
        fetch sCursor
          into userLessObject;
        exit when sCursor%notfound;
        FMSP_Exec(pIn_vSql      => 'drop table ' || userLessObject.OWNER || '.' ||
                                   userLessObject.OBJECT_NAME,
                  pOut_nSqlCode => nSqlCode);
      end loop;
      close sCursor;
      Fmp_Log.LOGEND;
    end;
  End FMSP_CLEARTBMID;
end FMP_ClEARTBMID;
/
