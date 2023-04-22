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

create or replace table PIM_ELIG_CCI as
with cci_stk as (
       select patid, 
           case when substr(dx,1,3) in ('410','I21','I22') then 'acute_mi'
                when substr(dx,1,3) in ('412') or substr(dx,1,5) in ('I25.2') then 'history_mi'
                when substr(dx,1,3) in ('428','I43','I50') or 
                     substr(dx,1,5) in ('425.4','425.5','425.6','425.7','425.8','425.9','I099','I11.0','I13.0','I13.2','I25.5','I42.0','P29.0','I42.5','I42.6','I42.7','I42.8','I42.9') or 
                     dx in ('398.91','402.01','402.11','402.91','404.01','404.03','404.11','404.13','404.91','404.93') then 'chf'
                when substr(dx,1,3) in ('440','441','I70','I71') or 
                     substr(dx,1,5) in ('093.0','447.1','557.1','557.9','V43.4','443.1','443.2','443.4','443.5','443.6','443.7','443.8','443.9','I73.1','I73.8','I73.9','I77.1','I79.0','I79.2','K55.1','K55.8','K55.9','Z95.8','Z95.9') then 'pvd'
                when substr(dx,1,3) in ('430','431','432','433','435','436','437','438','G45','G46','I60','I61','I62','I63','I64','I65','I66','I67','I68','I69') or 
                     substr(dx,1,5) in ('H34.0') or 
                     dx in ('362.34') then 'cvd'
                when substr(dx,1,3) in ('290','G30','F00','F01','F02','F03') or 
                     substr(dx,1,5) in ('294.1','331.2','F05.1','G31.1') then 'dementia'
                when substr(dx,1,3) in ('490','491','492','493','494','495','496','497','498','499','500','501','502','503','504','505','J40','J41','J42','J43','J44','J45','J46','J47') or 
                     substr(dx,1,5) in ('416.8','416.9','506.4','508.1','508.8','I27.8','I27.9','J68.4','J70.1','J70.3') then 'copd'
                when substr(dx,1,3) in ('342','343','G81','G82') or 
                     substr(dx,1,5) in ('334.1','344.9','344.0','344.1','344.2','344.3','344.4','344.5','344.6','G04.1','G11.4','G80.1','G80.2','G83.9','G83.0','G83.1','G83.2','G83.3','G83.4') then 'paralysis'
                when substr(dx,1,5) in ('250.0','250.1','250.2','250.3','250.8','250.9','E10.0','E10.1','E10.6','E10.8','E10.9','E11.0','E11.1','E11.6','E11.8','E11.9','E13.0','E13.1','E13.6','E13.8','E13.9') then 'diabetes'
                when substr(dx,1,5) in ('250.4','250.5','250.6','250.7','E10.2','E10.3','E10.4','E10.5','E10.7','E11.2','E11.3','E11.4','E11.5','E11.7','E13.2','E13.3','E13.4','E13.5','E13.7') then 'diabetes_comp'
                when substr(dx,1,3) in ('N18','N19','582','585','586','V56') or 
                     substr(dx,1,5) in ('583.0','583.1','583.2','583.3','583.4','583.5','583.6','583.7','I12.0','I13.1','N25.0','Z94.0','Z99.2','N03.2','N03.3','N03.4','N03.5','N03.6','N03.7','N05.2','N05.3','N05.4','N05.5','N05.6','N05.7','Z49.0','Z49.1','Z49.2','588.0','V42.0','V45.1') or 
                     dx in ('403.01','403.11','403.91','404.02','404.03','404.12','404.13','404.92','404.93') then 'renal_disease'
                when substr(dx,1,3) in ('B18','K73','K74','570','571') or 
                     substr(dx,1,5) in ('K70.9','K71.7','K76.0','K76.8','K76.9','Z94.4','K70.0','K70.1','K70.2','K70.3','K71.3','K71.4','K71.5','K76.2','K76.3','K76.4','070.6','070.9','573.3','573.4','573.8','573.9','V42.7') or 
                     dx in ('070.22','070.23','070.32','070.33','070.44','070.54') then 'mild_liver_disease'
                when substr(dx,1,5) in ('456.0','456.1','456.2','572.2','572.3','572.4','572.5','572.6','572.7','572.8','I85.0','I85.9','I86.4','I98.2','K70.4','K71.1','K72.1','K72.9','K76.5','K76.6','K76.7') then 'liver_disease'
                when substr(dx,1,3) in ('531','532','533','534','K25','K26','K27','K28') then 'ulcers'
                when substr(dx,1,3) in ('M05','M06','725','M32','M33','M34') or 
                     substr(dx,1,5) in ('446.5','714.8','710.0','710.1','710.2','710.3','710.4','714.0','714.1','714.2','M31.5','M35.1','M35.3','M36.0') then 'rheum_disease'
                when substr(dx,1,3) in ('042','043','044','B20','B21','B22','B24') then 'aids'
           else 'others' end as cci_cat,
           days_to_index      
       from PIM_ELIG_DX
), cci_ord as (
       select patid, cci_cat, 
              row_number() over (partition by patid order by days_to_index) as cci_cat_idx
       from cci_stk
)
select * from (
       select patid, cci_cat, cci_cat_idx as ind 
       from cci_ord where cci_cat_idx = 1
)
pivot 
(
       max(ind) for cci_cat 
       in ('patid','acute_mi','history_mi','chf','pvd','cvd','copd','dementia','paralysis','diabetes','diabetes_comp',
           'renal_disease','mild_liver_disease','liver_disease','ulcers','rheum_disease','aids')
) 
       as p (patid,acute_mi,history_mi,chf,pvd,cvd,copd,dementia,paralysis,diabetes,diabetes_comp,
             renal_disease,mild_liver_disease,liver_disease,ulcers,rheum_disease,aids)
order by patid
;

select * from PIM_ELIG_CCI limit 10;

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

create or replace table PIM_CASE_CTRL_ASET as 
select cs.PATID, 
       d.AGE,
       d.SEX,
       d.RACE,
       d.HISPANIC,
       coalesce(cci.acute_mi,0) as acute_mi,
       coalesce(cci.history_mi,0) as history_mi,
       coalesce(cci.chf,0) as chf,
       coalesce(cci.pvd,0) as pvd,
       coalesce(cci.cvd,0) as cvd,
       coalesce(cci.copd,0) as copd,
       coalesce(cci.dementia,0) as dementia,
       coalesce(cci.paralysis,0) as paralysis,
       coalesce(cci.diabetes,0) as diabetes,
       coalesce(cci.diabetes_comp,0) as diabetes_comp,
       coalesce(cci.renal_disease,0) as renal_disease,
       coalesce(cci.mild_liver_disease,0) as mild_liver_disease,
       coalesce(cci.liver_disease,0) as liver_disease,
       coalesce(cci.ulcers,0) as ulcers,
       coalesce(cci.rheum_disease,0) as rheum_disease,
       coalesce(cci.aids,0) as aids,
       1 as PIM1_ind,
       case when CLS0 > 1 then 1 else 0 end as PIM2_ind,
       CLS0 as PIM_CNT, 
       case when coalesce(d.days_to_death,d.days_to_censor) >= 365.25*5 then 1 else 0 end as MORT_5YR,
       coalesce(d.days_to_death,d.days_to_censor) as days_to_endpoint, 
       case when d.days_to_death is null then 0 else 1 end as endpoint_status
from PIM_CASE_ELIG_PVT cs
join PIM_ELIG d on cs.patid = d.patid
left join PIM_ELIG_CCI cci on cs.patid = cci.patid
union
select d.PATID, 
       d.AGE,
       d.SEX,
       d.RACE,
       d.HISPANIC,
       coalesce(cci.acute_mi,0) as acute_mi,
       coalesce(cci.history_mi,0) as history_mi,
       coalesce(cci.chf,0) as chf,
       coalesce(cci.pvd,0) as pvd,
       coalesce(cci.cvd,0) as cvd,
       coalesce(cci.copd,0) as copd,
       coalesce(cci.dementia,0) as dementia,
       coalesce(cci.paralysis,0) as paralysis,
       coalesce(cci.diabetes,0) as diabetes,
       coalesce(cci.diabetes_comp,0) as diabetes_comp,
       coalesce(cci.renal_disease,0) as renal_disease,
       coalesce(cci.mild_liver_disease,0) as mild_liver_disease,
       coalesce(cci.liver_disease,0) as liver_disease,
       coalesce(cci.ulcers,0) as ulcers,
       coalesce(cci.rheum_disease,0) as rheum_disease,
       coalesce(cci.aids,0) as aids,
       0 as PIM1_ind,
       0 as PIM2_ind,
       0 as PIM_CNT, 
       case when coalesce(d.days_to_death,d.days_to_censor) >= 365.25*5 then 1 else 0 end as MORT_5YR,
       coalesce(d.days_to_death,d.days_to_censor) as days_to_endpoint, 
       case when d.days_to_death is null then 0 else 1 end as endpoint_status
from PIM_ELIG d 
left join PIM_ELIG_CCI cci on d.patid = cci.patid
where not exists (
       select 1 from PIM_CASE_ELIG_PVT cs
       where d.patid = cs.patid
);

select * from PIM_CASE_CTRL_ASET limit 10;

/* https://healthcaredelivery.cancer.gov/seermedicare/considerations/NCI.comorbidity.macro.sas */
create or replace table PIM_CASE_CTRL_ASET2 as
select a.*, 
       1*((a.acute_mi+a.history_mi+1)/abs(a.acute_mi+a.history_mi+1)) 
       + 1*(a.chf) 
       + 1*(a.pvd) 
       + 1*(a.cvd) 
       + 1*(a.copd) 
       + 1*(a.dementia) 
       + 2*(a.paralysis)
       + 1*(a.diabetes*(1-a.diabetes_comp)) 
       + 2*(a.diabetes_comp) 
       + 2*(a.renal_disease) 
       + 1*(a.mild_liver_disease*(1-a.liver_disease)) 
       + 3*(a.liver_disease)
       + 1*(a.ulcers) 
       + 1*(a.rheum_disease) 
       + 6*(a.aids) 
       as cci
from PIM_CASE_CTRL_ASET a
;

select * from PIM_CASE_CTRL_ASET2 limit 10;


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

