SELECT 
  week,
  CASE 
    WHEN (churned + contraction) != 0 THEN 
      ROUND((-1.0 * (new + resurrected + expansion)::numeric / (churned + contraction)::numeric)::numeric, 2)
    ELSE 0 
  END as quick_ratio
FROM (
  {wrr_query}
) wrr_data
WHERE week <= date_trunc('week', current_date at time zone 'America/Sao_Paulo')
ORDER BY week; 