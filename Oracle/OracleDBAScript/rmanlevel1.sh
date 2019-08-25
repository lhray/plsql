#!/bin/bash
export ORACLE_BASE=/u01/app/oracle
export ORACLE_HOME=/u01/app/oracle/product/11.2.0.3
export ORACLE_SID=fmorcl
export PATH=$ORACLE_HOME/bin:$PATH

rman target / log=/data2/flash_recovery_area/FMORCL/autobackup/backuplevel1.log <<EOF
RUN {
CONFIGURE RETENTION POLICY TO REDUNDANCY 1;
CONFIGURE CONTROLFILE AUTOBACKUP ON;
ALLOCATE CHANNEL ch00 TYPE DISK;
ALLOCATE CHANNEL ch01 TYPE DISK;
BACKUP  INCREMENTAL LEVEL=1    SKIP INACCESSIBLE    TAG hot_db_bk_level1     FILESPERSET 5 FORMAT '/data2/flash_recovery_area/FMORCL/autobackup/Backup_%d_%s_%p_%t'  DATABASE  plus archivelog FORMAT '/data2/flash_recovery_area/FMORCL/autobackup/Backup_arc_%U' delete all input;
RELEASE CHANNEL ch00;
RELEASE CHANNEL ch01;
delete noprompt obsolete;
delete noprompt expired backup;
}
exit;
EOF