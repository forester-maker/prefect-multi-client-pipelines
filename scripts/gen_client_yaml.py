"""One-off generator for the demo client files (kept so the feed lists are reproducible)."""
import sys

ERP = ["customer_orders", "customer_order_lines", "shop_orders", "shop_order_operations", "inventory_parts",
       "inventory_part_in_stock", "purchase_orders", "purchase_order_lines", "suppliers", "customers",
       "work_orders", "work_order_tasks", "equipment_objects", "sales_parts", "customer_invoices",
       "supplier_invoices", "projects", "project_activities", "employees", "cost_centers", "sales_quotations",
       "shipments", "shipment_lines", "handling_units", "material_requisitions", "inventory_transactions",
       "pm_actions", "fault_reports", "service_contracts", "maintenance_plans"]
SAP = ["mara", "marc", "makt", "vbak", "vbap", "likp", "lips", "ekko", "ekpo", "mseg", "mkpf", "bkpf", "bseg",
       "kna1", "lfa1", "aufk", "afko", "afpo", "equi", "iflot", "qmel", "qmfe", "vbrk", "vbrp", "mard"]

def feed(name, source, cadence, rows, busy=False):
    s = f"  - {{name: {name}, source: {source}, cadence: {cadence}, rows: {rows}"
    return s + (", busy: true}" if busy else "}")

def acme():
    lines = ["name: acme-manufacturing", "display_name: Acme Manufacturing",
             "azure_subscription: acme-prod-eun-0042   # client's own subscription; our worker runs here",
             "lake_container: acme-lake                 # ADLS Gen2 container (PVC on the demo cluster)",
             "timezone: Europe/London", "residency: standard",
             "model_inputs: [customer_orders, work_orders, inventory_parts]   # silver fans in on these three", "feeds:"]
    i = 0
    for n in ERP[:30]:
        rows = [1200, 800, 2400, 600, 1500][i % 5]
        lines.append(feed(n, "erp_cloud", "15m", rows, busy=(n == "shop_order_operations")))
        i += 1
    for n in SAP[:20]:
        rows = [900, 400, 1800, 700][i % 4]
        lines.append(feed(n, "sap", "15m", rows, busy=(n == "vbap")))
        i += 1
    for n in SAP[20:25]:
        lines.append(feed(n, "sap", "daily", 5000))
    return "\n".join(lines) + "\n"

def boreal():
    lines = ["name: boreal-energy", "display_name: Boreal Energy",
             "azure_subscription: boreal-prd-noe-0007",
             "lake_container: boreal-lake",
             "timezone: Europe/Oslo",
             "residency: strict                          # run logs never leave the client's cluster",
             "model_inputs: [customer_orders, work_orders, inventory_parts]", "feeds:"]
    for i, n in enumerate(["customer_orders", "customer_order_lines", "inventory_parts", "inventory_part_in_stock",
                           "work_orders", "work_order_tasks", "equipment_objects", "pm_actions", "fault_reports",
                           "maintenance_plans", "purchase_orders", "purchase_order_lines", "suppliers", "employees",
                           "service_contracts"]):
        lines.append(feed(n, "erp_cloud", "15m", [700, 1100, 500, 1600][i % 4], busy=(n == "fault_reports")))
    for n in ["bkpf", "bseg", "kna1", "lfa1", "equi"]:
        lines.append(feed(n, "sap", "daily", 3000))
    return "\n".join(lines) + "\n"

def helix():
    lines = ["name: helix-automotive", "display_name: Helix Automotive",
             "azure_subscription: helix-prod-weu-0113",
             "lake_container: helix-lake",
             "timezone: Europe/Berlin", "residency: standard",
             "model_inputs: [customer_orders, work_orders, inventory_parts]", "feeds:"]
    for i, n in enumerate(["customer_orders", "customer_order_lines", "shop_orders", "inventory_parts",
                           "work_orders", "equipment_objects", "suppliers", "customers"]):
        lines.append(feed(n, "erp_cloud", "15m", [900, 1300, 600][i % 3], busy=(n == "shop_orders")))
    for n in ["mara", "vbak", "vbap", "ekko"]:
        lines.append(feed(n, "sap", "15m", 800))
    return "\n".join(lines) + "\n"

if __name__ == "__main__":
    which = sys.argv[1]
    print({"acme": acme, "boreal": boreal, "helix": helix}[which](), end="")
