SELECT 
  day,
  CASE 
    WHEN churned != 0 THEN 
      ROUND((-1.0 * (new + resurrected)::numeric / churned::numeric)::numeric, 2)
    ELSE 0 
  END as quick_ratio
FROM (
  {dau_query}
) dau_data
WHERE day <= date_trunc('day', current_date at time zone 'America/Sao_Paulo')
ORDER BY day; 