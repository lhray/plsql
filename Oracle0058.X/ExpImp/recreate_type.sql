declare                                                            
  v_strsql varchar2(1000) := '';                                                                         
  icount   number := 0;                                                                                  
begin                                                                                                    
  icount := 0;                                                                                           
  select count(1)                                                                                        
    into icount                                                                                          
    from user_objects t                                                                                  
   where t.OBJECT_TYPE = 'TYPE'                                                                          
     and t.OBJECT_NAME = 'FMT_TYPE';                                                                     
  if icount > 0 then                                                                                     
    execute immediate 'drop type FMT_TYPE force';                                                        
  end if;                                                                                                
                                                                                                         
  v_strsql := 'create or replace type FMT_type is table of varchar2(100)';                               
  execute immediate v_strsql;                                                                            
                                                                                                         
  icount := 0;                                                                                           
  select count(1)                                                                                        
    into icount                                                                                          
    from user_objects t                                                                                  
   where t.OBJECT_TYPE = 'TYPE'                                                                          
     and t.OBJECT_NAME = 'FMT_NODE_TIMESERIES';                                                          
  if icount > 0 then                                                                                     
    execute immediate 'drop type FMT_NODE_TIMESERIES force';                                             
  end if;                                                                                                
                                                                                                         
  v_strsql := 'create or replace type FMT_node_timeseries is table of varchar2(100)';                    
  execute immediate v_strsql;                                                                            
                                                                                                         
  icount := 0;                                                                                           
  select count(1)                                                                                        
    into icount                                                                                          
    from user_objects t                                                                                  
   where t.OBJECT_TYPE = 'TYPE'                                                                          
     and t.OBJECT_NAME = 'FMT_OBJ_NODEID';                                                               
  if icount > 0 then                                                                                     
    execute immediate 'drop type FMT_OBJ_NODEID force';                                                  
  end if;                                                                                                
                                                                                                         
  v_strsql := 'create or replace type FMT_obj_nodeid is object(id number)';                              
  execute immediate v_strsql;                                                                            
                                                                                                         
  icount := 0;                                                                                           
  select count(1)                                                                                        
    into icount                                                                                          
    from user_objects t                                                                                  
   where t.OBJECT_TYPE = 'TYPE'                                                                          
     and t.OBJECT_NAME = 'FMT_NEST_TAB_NODEID';                                                          
  if icount > 0 then                                                                                     
    execute immediate 'drop type FMT_NEST_TAB_NODEID force';                                             
  end if;                                                                                                
                                                                                                         
  v_strsql := 'create or replace type FMT_nest_tab_nodeid is table  of FMT_obj_nodeid';                  
  execute immediate v_strsql;                                                                            
                                                                                                         
  icount := 0;                                                                                           
  select count(1)                                                                                        
    into icount                                                                                          
    from user_objects t                                                                                  
   where t.OBJECT_TYPE = 'TYPE'                                                                          
     and t.OBJECT_NAME = 'FMT_NODEATTRIVALUE';                                                           
  if icount > 0 then                                                                                     
    execute immediate 'drop type FMT_NODEATTRIVALUE force';                                              
  end if;                                                                                                
                                                                                                         
  v_strsql := 'create or replace type FMT_NodeAttriValue is object (NodeID number, AttriValueID number)';
  execute immediate v_strsql;                                                                            
                                                                                                         
  icount := 0;                                                                                           
  select count(1)                                                                                        
    into icount                                                                                          
    from user_objects t                                                                                  
   where t.OBJECT_TYPE = 'TYPE'                                                                          
     and t.OBJECT_NAME = 'FMT_NODEATTRIVALUE_ARRAY';                                                     
  if icount > 0 then                                                                                     
    execute immediate 'drop type FMT_NodeAttriValue_Array force';                                        
  end if;                                                                                                
                                                                                                         
  v_strsql := 'create or replace type FMT_NodeAttriValue_Array is table of FMT_NodeAttriValue';          
  execute immediate v_strsql;   
  

  icount := 0;                                                                                           
  select count(1)                                                                                        
    into icount                                                                                          
    from user_objects t                                                                                  
   where t.OBJECT_TYPE = 'TYPE'                                                                          
     and t.OBJECT_NAME = 'FMT_TNLISTS';                                                     
  if icount > 0 then                                                                                     
    execute immediate 'drop type FMT_TNLISTS force';                                        
  end if;                                                                                                
                                                                                                         
  v_strsql := 'create or replace type FMT_tnlists is table of number';          
  execute immediate v_strsql; 

  icount := 0;                                                                                           
  select count(1)                                                                                        
    into icount                                                                                          
    from user_objects t                                                                                  
   where t.OBJECT_TYPE = 'TYPE'                                                                          
     and t.OBJECT_NAME = 'FMT_NODETS';                                                     
  if icount > 0 then                                                                                     
    execute immediate 'drop type FMT_NODETS force';                                        
  end if;                                                                                                
                                                                                                         
  v_strsql := 'create or replace type FMT_NODETS as object
		(
		  AGGNODE VARCHAR2(200),
		  product VARCHAR2(60),
		  sales   VARCHAR2(60),
		  trade   VARCHAR2(60),
		  yy      VARCHAR2(4),
		  mm      VARCHAR2(2),
		  t_data  number
		)';          
  execute immediate v_strsql; 
  
  icount := 0;                                                                                           
  select count(1)                                                                                        
    into icount                                                                                          
    from user_objects t                                                                                  
   where t.OBJECT_TYPE = 'TYPE'                                                                          
     and t.OBJECT_NAME = 'FMT_NODETIMESERIES';                                                     
  if icount > 0 then                                                                                     
    execute immediate 'drop type FMT_NODETIMESERIES force';                                        
  end if;                                                                                                
                                                                                                         
  v_strsql := 'create or replace type FMT_NODETIMESERIES is table of FMT_NODETS';          
  execute immediate v_strsql; 
  
end;
/ 
exit