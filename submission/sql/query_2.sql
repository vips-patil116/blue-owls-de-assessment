WITH seller_order_metrics AS (
    SELECT
        ds.seller_id,
        ds.seller_state,
        foi.order_id,
        SUM(COALESCE(foi.price, 0) + COALESCE(foi.freight_value, 0)) AS order_item_revenue,
        MAX(CASE WHEN foi.is_late_delivery = TRUE THEN 1.0 ELSE 0.0 END) AS is_late_order,
        AVG(CAST(foi.days_delivery_vs_estimate AS DOUBLE)) AS avg_days_vs_estimate
    FROM fact_order_items AS foi
    INNER JOIN dim_sellers AS ds
        ON foi.seller_key = ds.seller_key
    GROUP BY
        ds.seller_id,
        ds.seller_state,
        foi.order_id
),
seller_metrics AS (
    SELECT
        seller_id,
        seller_state,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(order_item_revenue), 2) AS total_revenue,
        AVG(is_late_order) AS late_delivery_rate,
        AVG(avg_days_vs_estimate) AS avg_days_vs_estimate
    FROM seller_order_metrics
    GROUP BY
        seller_id,
        seller_state
    HAVING COUNT(DISTINCT order_id) >= 20
),
scored AS (
    SELECT
        seller_id,
        seller_state,
        total_orders,
        total_revenue,
        ROUND(late_delivery_rate, 4) AS late_delivery_rate,
        ROUND(avg_days_vs_estimate, 2) AS avg_days_vs_estimate,
        PERCENT_RANK() OVER (ORDER BY late_delivery_rate DESC NULLS LAST) AS on_time_pctl,
        PERCENT_RANK() OVER (ORDER BY avg_days_vs_estimate DESC NULLS LAST) AS speed_pctl,
        PERCENT_RANK() OVER (ORDER BY total_revenue ASC) AS revenue_pctl
    FROM seller_metrics
),
final_scores AS (
    SELECT
        seller_id,
        seller_state,
        total_orders,
        total_revenue,
        late_delivery_rate,
        avg_days_vs_estimate,
        ROUND(1 - on_time_pctl, 4) AS on_time_pctl,
        ROUND(1 - speed_pctl, 4) AS speed_pctl,
        ROUND(revenue_pctl, 4) AS revenue_pctl,
        ROUND(((1 - on_time_pctl) * 0.4) + ((1 - speed_pctl) * 0.3) + (revenue_pctl * 0.3), 4) AS composite_score
    FROM scored
)
SELECT
    seller_id,
    seller_state,
    total_orders,
    total_revenue,
    late_delivery_rate,
    avg_days_vs_estimate,
    on_time_pctl,
    speed_pctl,
    revenue_pctl,
    composite_score,
    DENSE_RANK() OVER (
        ORDER BY composite_score DESC, total_revenue DESC, seller_id ASC
    ) AS overall_rank
FROM final_scores
ORDER BY overall_rank, seller_id;
