WITH monthly_category_revenue AS (
    SELECT
        dp.product_category_name,
        YEAR(foi.order_date) AS year,
        MONTH(foi.order_date) AS month,
        COUNT(*) AS transaction_count,
        ROUND(SUM(COALESCE(foi.price, 0) + COALESCE(foi.freight_value, 0)), 2) AS monthly_revenue
    FROM fact_order_items AS foi
    INNER JOIN dim_products AS dp
        ON foi.product_key = dp.product_key
    GROUP BY
        dp.product_category_name,
        YEAR(foi.order_date),
        MONTH(foi.order_date)
),
eligible_months AS (
    SELECT *
    FROM monthly_category_revenue
    WHERE transaction_count >= 10
),
top_categories AS (
    SELECT product_category_name
    FROM (
        SELECT
            product_category_name,
            SUM(monthly_revenue) AS total_revenue,
            DENSE_RANK() OVER (
                ORDER BY SUM(monthly_revenue) DESC
            ) AS revenue_rank
        FROM eligible_months
        GROUP BY product_category_name
    ) ranked
    WHERE revenue_rank <= 5
),
ranked_months AS (
    SELECT
        em.product_category_name,
        em.year,
        em.month,
        em.monthly_revenue,
        DENSE_RANK() OVER (
            PARTITION BY em.year, em.month
            ORDER BY em.monthly_revenue DESC, em.product_category_name ASC
        ) AS monthly_rank,
        LAG(em.monthly_revenue) OVER (
            PARTITION BY em.product_category_name
            ORDER BY em.year, em.month
        ) AS previous_month_revenue,
        AVG(em.monthly_revenue) OVER (
            PARTITION BY em.product_category_name
            ORDER BY em.year, em.month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3m_avg_revenue
    FROM eligible_months em
    INNER JOIN top_categories tc
        ON em.product_category_name = tc.product_category_name
)
SELECT
    product_category_name,
    year,
    month,
    monthly_revenue,
    monthly_rank,
    CASE
        WHEN previous_month_revenue IS NULL OR previous_month_revenue = 0 THEN NULL
        ELSE ROUND(((monthly_revenue - previous_month_revenue) / previous_month_revenue) * 100, 2)
    END AS mom_growth_pct,
    ROUND(rolling_3m_avg_revenue, 2) AS rolling_3m_avg_revenue
FROM ranked_months
ORDER BY year, month, monthly_rank, product_category_name;
