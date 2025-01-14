SELECT 
  week,
  CASE 
    WHEN churned != 0 THEN 
      ROUND((-1.0 * (new + resurrected)::numeric / churned::numeric)::numeric, 2)
    ELSE 0 
  END as quick_ratio
FROM (
  {wau_query}
) wau_data
WHERE week <= date_trunc('week', current_date at time zone 'America/Sao_Paulo')
ORDER BY week; 