create or replace package p_ParallelTaskMgr is
  v_parallel_cnt number;
  e_parallel_err exception;

  procedure sp_ParallelExecTaskBySQL(p_TaskName       in varchar2,
                                     p_ChunkSQL       in varchar2,
                                     p_ParallelDegree in number default NULL,
                                     p_SQLStmt        in clob,
                                     p_sqlcode        out number);
  procedure sp_ParallelExecTaskByCol(p_TaskName       in varchar2,
                                     p_TableName      in varchar2,
                                     p_ColumnName     in varchar2,
                                     p_ChunkSize      in number,
                                     p_ParallelDegree in number default NULL,
                                     p_SQLStmt        in clob,
                                     p_sqlcode        out number);
end p_ParallelTaskMgr;
/
create or replace package body p_ParallelTaskMgr is

  procedure sp_ParallelExecTaskBySQL(p_TaskName       in varchar2,
                                     p_ChunkSQL       in varchar2,
                                     p_ParallelDegree in number default NULL,
                                     p_SQLStmt        in clob,
                                     p_sqlcode        out number) as
    v_task_name varchar2(400);
    v_status    number;
  begin
    p_sqlcode := 0;

    v_task_name := dbms_parallel_execute.generate_task_name(p_TaskName);
    dbms_parallel_execute.create_task(v_task_name);
    dbms_parallel_execute.create_chunks_by_SQL(task_name => v_task_name,
                                               sql_stmt  => p_ChunkSQL,
                                               by_rowid  => false);
    dbms_parallel_execute.run_task(task_name      => v_task_name,
                                   sql_stmt       => p_SQLStmt,
                                   language_flag  => DBMS_SQL.NATIVE,
                                   parallel_level => p_ParallelDegree);
    v_status := DBMS_PARALLEL_EXECUTE.TASK_STATUS(v_task_name);
    if v_status <> dbms_parallel_execute.FINISHED then
      raise e_parallel_err;
    end if;
    dbms_parallel_execute.drop_task(v_task_name);
  exception
    when e_parallel_err then
      p_sqlcode := -20998;
      Fmp_Log.LOGERROR;
      dbms_parallel_execute.drop_task(v_task_name);
      raise;
    when others then
      dbms_parallel_execute.drop_task(v_task_name);
      p_Sqlcode := sqlcode;
      Fmp_Log.LOGERROR;
      raise;
  end;

  procedure sp_ParallelExecTaskByCol(p_TaskName       in varchar2,
                                     p_TableName      in varchar2,
                                     p_ColumnName     in varchar2,
                                     p_ChunkSize      in number,
                                     p_ParallelDegree in number default NULL,
                                     p_SQLStmt        in clob,
                                     p_sqlcode        out number) as
    v_task_name varchar2(400);
    v_status    number;
  begin
    v_task_name := dbms_parallel_execute.generate_task_name(p_TaskName);
    dbms_parallel_execute.create_task(v_task_name);
    dbms_parallel_execute.create_chunks_by_number_col(task_name    => v_task_name,
                                                      table_owner  => user,
                                                      table_name   => p_TableName,
                                                      table_column => p_ColumnName,
                                                      chunk_size   => p_ChunkSize);

    dbms_parallel_execute.run_task(task_name      => v_task_name,
                                   sql_stmt       => p_SQLStmt,
                                   language_flag  => DBMS_SQL.NATIVE,
                                   parallel_level => p_ParallelDegree);
    v_status := DBMS_PARALLEL_EXECUTE.TASK_STATUS(v_task_name);
    if v_status <> dbms_parallel_execute.FINISHED then
      raise e_parallel_err;
    end if;
    dbms_parallel_execute.drop_task(v_task_name);
  exception
    when e_parallel_err then
      p_sqlcode := -20998;
      Fmp_Log.LOGERROR;
      dbms_parallel_execute.drop_task(v_task_name);
      raise;
    when others then
      dbms_parallel_execute.drop_task(v_task_name);
      p_Sqlcode := sqlcode;
      Fmp_Log.LOGERROR;
      raise;
  end;
begin
  select nvl(max(value), 1)
    into v_parallel_cnt
    from fm_config
   where id = 1;

end p_ParallelTaskMgr;
/
