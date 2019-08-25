declare
begin
for CUR_SEQ in (select t.sequence_name from user_sequences t where t.sequence_name like 'FM$$/_%' ESCAPE '/') loop
  execute immediate 'drop sequence ' || CUR_SEQ.SEQUENCE_NAME;
end loop;
end;
/