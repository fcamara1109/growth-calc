SELECT 
  month,
  retained,
  preceding_mau,
  CASE 
    WHEN preceding_mau > 0 THEN 
      ROUND((retained::numeric / preceding_mau::numeric) * 100, 2)
    ELSE 0 
  END as retention_rate
FROM (
  {mau_query}
) mau_data
WHERE month <= date_trunc('month', current_date at time zone 'America/Sao_Paulo')
ORDER BY month; 