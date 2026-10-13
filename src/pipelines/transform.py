"""DuckDB transformations: bronze -> silver -> gold, all inside the client's lake."""

from __future__ import annotations

import duckdb
import pyarrow as pa

from pipelines.lake import Lake


def silver_operations_snapshot(lake: Lake, model_inputs: list[str]) -> pa.Table:
    """Join the model's input feeds into one operational snapshot per part and site."""
    con = duckdb.connect()
    for feed in model_inputs:
        con.execute(f"CREATE VIEW {feed} AS SELECT * FROM read_parquet('{lake.glob('bronze', feed)}')")
    orders, work, stock = model_inputs  # customer demand, maintenance demand, stock position
    table = con.execute(
        f"""
        WITH demand AS (
            SELECT site, part_no, sum(qty) AS ordered_qty, sum(amount) AS ordered_value, count(*) AS orders
            FROM {orders} GROUP BY 1, 2),
        maint AS (
            SELECT site, part_no, sum(qty) AS maintenance_qty FROM {work} WHERE status <> 'CLOSED' GROUP BY 1, 2),
        stock AS (
            SELECT site, part_no, sum(qty) AS on_hand FROM {stock} GROUP BY 1, 2)
        SELECT coalesce(d.site, m.site, s.site) AS site,
               coalesce(d.part_no, m.part_no, s.part_no) AS part_no,
               coalesce(ordered_qty, 0) AS ordered_qty,
               coalesce(ordered_value, 0) AS ordered_value,
               coalesce(orders, 0) AS orders,
               coalesce(maintenance_qty, 0) AS maintenance_qty,
               coalesce(on_hand, 0) AS on_hand,
               coalesce(on_hand, 0) - coalesce(ordered_qty, 0) - coalesce(maintenance_qty, 0) AS projected_balance
        FROM demand d
        FULL OUTER JOIN maint m USING (site, part_no)
        FULL OUTER JOIN stock s USING (site, part_no)
        """
    ).fetch_arrow_table()
    lake.write("silver", "operations_snapshot", table)
    return table


def gold_site_kpis(lake: Lake) -> pa.Table:
    con = duckdb.connect()
    table = con.execute(
        f"""
        SELECT site,
               count(*)::BIGINT AS parts,
               sum(orders)::BIGINT AS open_orders,
               round(sum(ordered_value), 2)::DOUBLE AS order_value,
               sum(CASE WHEN projected_balance < 0 THEN 1 ELSE 0 END)::BIGINT AS parts_at_risk,
               round(100.0 * sum(CASE WHEN projected_balance >= 0 THEN 1 ELSE 0 END) / count(*), 1)::DOUBLE AS fill_rate_pct
        FROM read_parquet('{lake.glob('silver', 'operations_snapshot')}')
        GROUP BY site ORDER BY site
        """
    ).fetch_arrow_table()
    lake.write("gold", "site_kpis", table)
    return table
