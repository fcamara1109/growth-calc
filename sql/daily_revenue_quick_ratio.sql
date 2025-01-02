SELECT 
  day,
  CASE 
    WHEN (churned + contraction) != 0 THEN 
      ROUND((-1.0 * (new + resurrected + expansion)::numeric / (churned + contraction)::numeric)::numeric, 2)
    ELSE 0 
  END as quick_ratio
FROM (
  {drr_query}
) drr_data
WHERE day <= date_trunc('day', current_date at time zone 'America/Sao_Paulo')
ORDER BY day; 