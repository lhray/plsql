create or replace view fmv_disrms as
--*****************************************************************
    -- Description: dis /rms
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        29-12-2012     wfq           Created.
    -- **********************************************
select d.dis_em_addr dimensionID,null NO,r.rms_em_addr coefficientID,r.rms_cle Key,r.rms_desc Description
from dis d,rms r
where d.rms40_em_addr=r.rms_em_addr;
