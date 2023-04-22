/*             
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: pim_case_ctrl.sql
# Description: identifying exposed group with at least 2 different drugs 
#              from the same PIM drug class within study period
# Dependency: PIM_VS_RXNORM
*/

set cdm_schema = 'DEIDENTIFIED_PCORNET_CDM.CDM_2023_JAN';
set demographic = $cdm_schema || '.DEID_DEMOGRAPHIC';
set prescribing = $cdm_schema || '.DEID_PRESCRIBING';
set encounter = $cdm_schema || '.DEID_ENCOUNTER';
set death = $cdm_schema || '.DEID_DEATH';
set diagnosis = $cdm_schema || '.DEID_DIAGNOSIS';

set start_date = '2016-01-01';
set end_date = '2016-12-31';
set endpoint_date = '2019-12-31';

create or replace table PIM_ELIG as
select d.PATID,
       d.BIRTH_DATE,
       round(datediff(day,d.birth_date,$start_date)/365.25) as AGE,
       d.SEX,
       case when d.RACE = '05' then 'White'
            when d.RACE = '03' then 'AA'
            when d.RACE = '02' then 'Asian'
            when d.RACE = '01' then 'AI_AN'
            when d.RACE in ('NI','UN','07') or d.RACE is null then 'NI'
            else 'OT' 
       end as race,
       case when d.HISPANIC in ('Y','N') then d.HISPANIC 
            else 'NI' 
       end as hispanic,
       max(dth.DEATH_DATE::date) as death_date,
       datediff(day,'2017-01-01',max(dth.DEATH_DATE)) as days_to_death,
       max(coalesce(enc.discharge_date,enc.admit_date)) as censor_date,
       datediff(day,'2017-01-01',max(coalesce(enc.discharge_date::date,enc.admit_date::date))) as days_to_censor,
       min(p.rx_order_date) as index_date
from identifier($demographic) d 
join identifier($encounter) enc on d.patid = enc.patid
join identifier($prescribing) p on d.patid = p.patid
left join identifier($death) dth on d.patid = dth.patid
where p.rx_order_date between $start_date and $end_date and
      (dth.DEATH_DATE >= '2017-01-01' or dth.DEATH_DATE is null) and 
      enc.admit_date >= '2017-01-01' and 
      round(datediff(day,d.birth_date,$start_date)/365.25) >= 65
group by d.PATID, d.BIRTH_DATE,d.SEX,d.RACE,d.HISPANIC
;

select count(distinct patid) from PIM_ELIG;

select * from PIM_ELIG limit 10;

create or replace table PIM_ELIG_DX as 
with dx_stk as (
       select d.PATID,
              dx.DX,dx.DX_TYPE,
              coalesce(dx.DX_DATE,dx.ADMIT_DATE) as DX_DATE,
              d.index_date,
              row_number() over (partition by d.PATID,dx.DX order by datediff(day,coalesce(dx.DX_DATE,dx.ADMIT_DATE),d.index_date)) as DX_RECENT_IDX
       from PIM_ELIG d
       join identifier($diagnosis) dx on d.patid = dx.patid
       where datediff(day,coalesce(dx.DX_DATE,dx.ADMIT_DATE),d.index_date) > 0
)
select patid, 
       dx_type, dx, 
       index_date::date as index_date, 
       dx_date::date as dx_date,
       datediff(day,dx_date::date,index_date::date) as days_to_index
from dx_stk
where dx_recent_idx = 1
;

select * from PIM_ELIG_DX limit 10;


create or replace table PIM_CASE_ELIG as 
select d.PATID, 
       rxn.CODEGRP_LABEL as DRUG_CLASS, 
       'CLS' || dense_rank() over (order by rxn.CODEGRP) as drug_class_idx,
       count(distinct rxn.CODE) as DRUG_CNTS_BY_CLASS,
       count(distinct p.RX_ORDER_DATE) as PRESCRB_CNTS_BY_CLASS
from PIM_ELIG d 
join identifier($prescribing) p on d.patid = p.patid
join public.pim_vs_rxnorm rxn on rxn.CODE = p.RXNORM_CUI
where p.RX_ORDER_DATE between $start_date and $end_date
group by d.PATID, rxn.CODEGRP, rxn.CODEGRP_LABEL
union 
select d.PATID, 
       'Any class',
       'CLS0',
       count(distinct rxn.CODE) as DRUG_CNTS_BY_CLASS,
       count(distinct p.RX_ORDER_DATE) as PRESCRB_CNTS_BY_CLASS
from PIM_ELIG d 
join identifier($prescribing) p on d.patid = p.patid
join public.pim_vs_rxnorm rxn on rxn.CODE = p.RXNORM_CUI
where p.RX_ORDER_DATE between $start_date and $end_date
group by d.PATID
;

select count(distinct patid) from PIM_CASE_ELIG;
select drug_class_idx,DRUG_CLASS, count(distinct patid) from PIM_CASE_ELIG
group by drug_class_idx,DRUG_CLASS;


create or replace table PIM_CASE_ELIG_PVT as
with PIM_CASE_ELIG_sub as (
       select patid, drug_class_idx, drug_cnts_by_class
       from PIM_CASE_ELIG
)
select * from PIM_CASE_ELIG_sub
pivot 
(
       max(drug_cnts_by_class) for drug_class_idx 
       in ('CLS0','CLS1','CLS2','CLS3','CLS4','CLS5','CLS6','CLS7','CLS8','CLS9','CLS10',
           'CLS11','CLS12','CLS13','CLS14','CLS15','CLS16','CLS17','CLS18','CLS19','CLS20')
) 
       as p (PATID,CLS0,CLS1,CLS2,CLS3,CLS4,CLS5,CLS6,CLS7,CLS8,CLS9,CLS10,
             CLS11,CLS12,CLS13,CLS14,CLS15,CLS16,CLS17,CLS18,CLS19,CLS20)
order by PATID
;

create or replace table PIM_CASE_CTRL_TABLE1 as 
select cs.PATID, 
       d.AGE,
       d.SEX,
       d.RACE,
       d.HISPANIC,
       1 as PIM1_ind,
       case when CLS0 > 1 then 1 else 0 end as PIM2_ind,
       CLS0 as PIM_CNT, 
       case when coalesce(d.days_to_death,d.days_to_censor) >= 365.25*5 then 1 else 0 end as MORT_5YR,
       coalesce(d.days_to_death,d.days_to_censor) as days_to_endpoint, 
       case when d.days_to_death is null then 0 else 1 end as endpoint_status
from PIM_CASE_ELIG_PVT cs
join PIM_ELIG d on cs.patid = d.patid
union
select d.PATID, 
       d.AGE,
       d.SEX,
       d.RACE,
       d.HISPANIC,
       0 as PIM1_ind,
       0 as PIM2_ind,
       0 as PIM_CNT, 
       case when coalesce(d.days_to_death,d.days_to_censor) >= 365.25*5 then 1 else 0 end as MORT_5YR,
       coalesce(d.days_to_death,d.days_to_censor) as days_to_endpoint, 
       case when d.days_to_death is null then 0 else 1 end as endpoint_status
from PIM_ELIG d 
where not exists (
       select 1 from PIM_CASE_ELIG_PVT cs
       where d.patid = cs.patid
);


select * from PIM_CASE_CTRL_TABLE1 limit 10;

select PIM2_IND, count(distinct patid) from PIM_CASE_CTRL_TABLE1
group by PIM2_IND
;

select PIM1_IND, count(distinct patid) from PIM_CASE_CTRL_TABLE1
group by PIM1_IND
;

select PIM_CNT, count(distinct patid) from PIM_CASE_CTRL_TABLE1
group by PIM_CNT
order by PIM_CNT
;

