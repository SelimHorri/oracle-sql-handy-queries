
SELECT *
FROM (
  SELECT  date, 
          Total_Volume_4G, 
          groupname,
          case 
            when length(NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)) <= 11 then NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)
            when length(NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)) > 11 then NVL(SUBSTR(label, INSTR(label, '/')+1, length(label)), label)
            else NULL
          end as label,
          rank() over ( partition by groupname, date order by Total_Volume_4G desc) rank_4g
  FROM
    WITH date_time as (
      SELECT  trunc(add_months(sysdate, -3), 'mm') startdate,
              trunc(add_months(sysdate, 0), 'mm') as enddate
    )
    SELECT  to_char(trunc(date, 'MM'), 'MM/YYYY') date,
            round(sum(bytes_up_4g_in + bytes_dn_4g_in + bytes_up_4g_off + bytes_dn_4g_off)/1000000,5) as Total_Volume_4G,
            groupname,
            case
              when length(NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)) <=11 then NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)
              when length(NVL(SUBSTR(label, 0, INSTR(label, '/')-1), label)) >11 then NVL(SUBSTR(label, INSTR(label, '/')+1, length(label)), label)
              else NULL
            end AS label
    FROM b2b_data.wng_daily_kpis, date_time
    WHERE 
        DATE >= startdate  AND DATE < enddate
    group by trunc(date, 'MM'), groupname, label
)
WHERE 
      rank_4g IN (1,2,3,4,5)
  AND total_volume_4g != 0
ORDER BY date desc, groupname, rank_4g;  


