SELECT 
  month,
  CASE 
    WHEN churned != 0 THEN 
      ROUND((-1.0 * (new + resurrected)::numeric / churned::numeric)::numeric, 2)
    ELSE 0 
  END as quick_ratio
FROM (
  {mau_query}
) mau_data
WHERE month <= date_trunc('month', current_date at time zone 'America/Sao_Paulo')
ORDER BY month; 