"""Shared SQL templates used by every report.

These six templates pull from the common inventory / usage / PLM tables and are
identical across reports. Each report's own ``queries.py`` imports
``BASE_TEMPLATES`` and may add report-specific templates on top.

``{location_filter}`` is substituted at runtime by ``db.fetch_tables``.
"""

BASE_TEMPLATES = {
    "inventory": r"""
        SELECT *
        FROM (
            SELECT Location, Item, ItemDescription, Active, Discontinued,
                   VendorItem, Vendor, VendorName,
                   ManufacturerNumber, StrippedManufacturerNumber, ManufacturerName,
                   StockUOM, DefaultBuyUOM, BuyUOMMultiplier, UnitCostInStockUOM,
                   AvailableQty, OnOrderQty, [update stamp], [report stamp],
                   ROW_NUMBER() OVER (
                       PARTITION BY [Location], Item
                       ORDER BY [report stamp] DESC
                   ) AS RK
            FROM [DM_MONTYNT\dli2].INVENTORY_LOCATION
            WHERE Location IN ({location_filter})
              AND Active = 'Yes'
              AND Discontinued = 'No'
        ) c
        WHERE RK = 1
    """,
    "usage": """
        SELECT Location, Item,
               SUM(QtyInLum) * 1.0 / 365 AS AverageDailyIssueOut
        FROM (
            SELECT *
            FROM plm.DailyIssueOutQty
            WHERE Location IN ({location_filter})
              AND trx_date BETWEEN DATEADD(DAY, -366, GETDATE()) AND GETDATE()
        ) c
        GROUP BY Location, Item
    """,
    "long_desc": r"""
        SELECT Item, Description3
        FROM [DM_MONTYNT\dli2].MDM_ITEM
    """,
    "plmlink": """
        SELECT [Item Group], Item, [Replace Item], [Stage]
        FROM plm.Itemlink
        WHERE Stage NOT IN ('Deleted', 'Completed', 'Pending Item Number')
    """,
    "plmusage": """
        SELECT [Item Group], rolling_daily_avg_7
        FROM PLM.PLMItemGroupBRRolling
        WHERE Location IN ({location_filter})
    """,
    "timestamp": r"""
        SELECT MAX([report stamp]) AS stamp
        FROM [DM_MONTYNT\dli2].INVENTORY_LOCATION
        WHERE Location IN ({location_filter})
          AND Active = 'Yes'
          AND Discontinued = 'No'
          AND [report stamp] >= DATEADD(DAY, -10, GETDATE())
    """,
}
