SELECT 
  week,
  retained,
  preceding_wau,
  CASE 
    WHEN preceding_wau > 0 THEN 
      ROUND((retained::numeric / preceding_wau::numeric) * 100, 2)
    ELSE 0 
  END as retention_rate
FROM (
  {wau_query}
) wau_data
WHERE week <= date_trunc('week', current_date at time zone 'America/Sao_Paulo')
ORDER BY week; 