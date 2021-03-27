
CREATE VIEW TOP_label_PER_COMPANY_MM AS
SELECT *
FROM (
  SELECT  date, 
          Total_Volume_4G, 
          groupname,
          CASE 
            WHEN length(NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)) <= 11 THEN NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)
            WHEN length(NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)) > 11 THEN NVL(SUBSTR(label, INSTR(label, '/')+1, length(label)), label)
            ELSE NULL
          END AS label,
          rank() over ( partition by groupname, date order by Total_Volume_4G desc) rank_4g
  FROM
    WITH date_time AS (
      SELECT  trunc(add_months(sysdate, -3), 'mm') startdate,
              trunc(add_months(sysdate, 0), 'mm') AS enddate
    )
    SELECT  to_char(trunc(date, 'MM'), 'MM/YYYY') date,
            round(sum(bytes_up_4g_in + bytes_dn_4g_in + bytes_up_4g_off + bytes_dn_4g_off)/1000000,5) AS Total_Volume_4G,
            groupname,
            CASE
              WHEN length(NVL(SUBSTR(label, 0, INSTR(label, '/') - 1), label)) <= 11 THEN NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)
              WHEN length(NVL(SUBSTR(label, 0, INSTR(label, '/') - 1), label)) > 11 THEN NVL(SUBSTR(label, INSTR(label, '/')+1, length(label)), label)
              ELSE NULL
            END AS label
    FROM b2b_data.wng_daily_kpis, date_time
    WHERE 
        DATE >= startdate  AND DATE < enddate
    GROUP BY trunc(date, 'MM'), groupname, label
)
WHERE 
      rank_4g IN (1,2,3,4,5)
  AND total_volume_4g != 0
ORDER BY date DESC, groupname, rank_4g;  


-- TEst example
SELECT  a.date,
        a.groupname,
        NVL(SUBSTR(label, 0, INSTR(label, '/') - 1), label) AS label,
        label AS original_label
FROM dummy_table a
WHERE NVL(SUBSTR(label, 0, INSTR(label, '/') - 1), label) = '311002174918536';
--logic before was pick the first number prior to the '/', but it appears there are some cases where it is after the '/'


--Select case allows to perform an if else statement logic within sql
--general example
SELECT  CASE 
            WHEN trunc(sysdate) + 1 = to_date('17-JUL-2019', 'DD-MON-YYYY') THEN 1
            WHEN trunc(sysdate) + 1 = to_date('19-JUL-2019', 'DD-MON-YYYY') THEN 2
            --named as test_var
            ELSE 0
        END AS test_var,
        sysdate
FROM dual;





--practical example
select 
    case
        --select from beginning of string(0) to '/'
        when length(NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)) <=11 then NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)
        --select from '/' to end of string
        when length(NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)) >11 then NVL(SUBSTR(label, INSTR(label, '/')+1, length(label)), label)
        else NULL
      end as test_var,
      date,
      groupname,
      label
from testdata
where length(NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label))>11;




 select * from (
   SELECT date,
   total,
   groupname, NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label) as label, rank() over ( partition by groupname, date order by total desc) ranking
   FROM (
     WITH date_time as (
       select trunc(add_months(sysdate, -3
       ), 'mm') startdate,
       trunc(add_months(sysdate, 0), 'mm') as enddate
     )
     SELECT to_char(trunc(date, 'MM'), 'MM/YYYY') date, round(sum(kpi1 + kpi2 + kpi3 + kpi4)/1000000,5) as total,
     groupname, NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label) as label
     FROM testdata, date_time
    WHERE  DATE >= startdate  AND DATE < enddate
     group by trunc(date, 'MM'), groupname, NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)
   )
 )
 where ranking IN (1,2,3,4,5) and total !=0
 order by  date desc, groupname, ranking;
 





