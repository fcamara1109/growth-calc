SELECT 
  day,
  retained,
  preceding_rev,
  CASE 
    WHEN preceding_rev > 0 THEN 
      ROUND((retained::numeric / preceding_rev::numeric) * 100, 2)
    ELSE 0 
  END as retention_rate
FROM (
  {drr_query}
) drr_data
WHERE day <= date_trunc('day', current_date at time zone 'America/Sao_Paulo')
ORDER BY day; 