create or replace package P_MostRecentUsed is
  /*********
  Created by JYLiu 11/15/2012   manage the user's history opened windows
  *********/
  --reset the order of the opened windows
  procedure SP_ExistedMostRecentUsedADD(p_user_130_em_addr   in number,
                                        p_old_user_data_type in number, --the  opened window
                                        p_min_user_data_type in number, --the current opened window
                                        p_sqlcode            out number,
                                        p_sqlmsg             out varchar2);
  --remove the oldest opened window record,add a new record
  procedure SP_NotExistedMostRecentUsedADD(p_user_130_em_addr   in number,
                                           p_user_data_size     in number,
                                           p_min_user_data_type in number,
                                           p_max_user_data_type in number,
                                           p_param_user_xmldata in blob,
                                           p_sqlcode            out number,
                                           p_sqlmsg             out varchar2);

end P_MostRecentUsed;
/
create or replace package body P_MostRecentUsed is
  procedure SP_ExistedMostRecentUsedADD(p_user_130_em_addr   in number,
                                        p_old_user_data_type in number,
                                        p_min_user_data_type in number,
                                        p_sqlcode            out number,
                                        p_sqlmsg             out varchar2) is
  
    v_user_dataID number;
  begin
    p_sqlcode := 0;
    p_sqlmsg  := '';
  
    update user_data t
       set t.user_data_type = p_min_user_data_type
     where t.user_130_em_addr = p_user_130_em_addr
       and t.user_data_type = p_old_user_data_type
    returning t.user_data_em_addr into v_user_dataID;
  
    update user_data t
       set t.user_data_type = t.user_data_type + 1
     where t.user_130_em_addr = p_user_130_em_addr
       and t.user_data_em_addr <> v_user_dataID
       and (t.user_data_type >= p_min_user_data_type and
           t.user_data_type < p_old_user_data_type);
    
  exception
    when NO_DATA_FOUND then
      
      p_sqlcode := sqlcode;
      p_sqlmsg  := sqlerrm;
      raise_application_error(p_constant.e_oraerr, sqlerrm);
    when others then
      
      p_sqlcode := sqlcode;
      p_sqlmsg  := sqlerrm;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure SP_NotExistedMostRecentUsedADD(p_user_130_em_addr   in number,
                                           p_user_data_size     in number,
                                           p_min_user_data_type in number,
                                           p_max_user_data_type in number,
                                           p_param_user_xmldata in blob,
                                           p_sqlcode            out number,
                                           p_sqlmsg             out varchar2) is
  
  
  begin
    p_sqlcode := 0;
    p_sqlmsg  := '';
  
    delete from user_data t
     where t.user_130_em_addr = p_user_130_em_addr
       and t.user_data_type = p_max_user_data_type;
  
    update user_data t
       set t.user_data_type = t.user_data_type + 1
     where t.user_130_em_addr = p_user_130_em_addr
       and (t.user_data_type >= p_min_user_data_type and
           t.user_data_type < p_max_user_data_type);
 
    insert into user_data
      (user_data_em_addr,
       user_data_type,
       user_data_size,
       XMLDATA,
       user_130_em_addr)
    values
      (seq_user_data.nextval,
       p_min_user_data_type,
       p_user_data_size,
       p_param_user_xmldata,
       p_user_130_em_addr);
    
  exception
    when NO_DATA_FOUND then
      
      p_sqlcode := sqlcode;
      p_sqlmsg  := sqlerrm;
      raise_application_error(p_constant.e_oraerr, sqlerrm);
    when others then
      
      p_sqlcode := sqlcode;
      p_sqlmsg  := sqlerrm;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;
begin
  null;
end P_MostRecentUsed;
/
