SELECT 
  day,
  retained,
  preceding_dau,
  CASE 
    WHEN preceding_dau > 0 THEN 
      ROUND((retained::numeric / preceding_dau::numeric) * 100, 2)
    ELSE 0 
  END as retention_rate
FROM (
  {dau_query}
) dau_data
WHERE day <= date_trunc('day', current_date at time zone 'America/Sao_Paulo')
ORDER BY day; 