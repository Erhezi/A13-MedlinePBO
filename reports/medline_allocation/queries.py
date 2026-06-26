"""SQL templates for the Medline Allocation report.

Reuses the shared ``BASE_TEMPLATES`` (inventory / usage / PLM) and adds two
report-specific reads:

- ``collect_item`` — resolve a Medline ``VendorItem`` to its internal ``Item``.
- ``LOST_ALLOC_SQL`` — read prior weeks of this report's own helper table from
  the SCSFileIngestor server (parameterized by ``min_yearweek``, NOT location).
"""

from src.queries import BASE_TEMPLATES

EXTRA_TEMPLATES = {
    "collect_item": r"""
        SELECT DISTINCT VendorItem, ItemNumber AS Item
        FROM [DM_MONTYNT\dli2].CONTRACTLINE
        WHERE itemtype = 'Itemmast'
          AND VendorName LIKE '%Medline%'
    """,
}

# Fetched from PRIME in one fetch_tables() call (collect_item has no
# {location_filter}, so the format() pass leaves it unchanged).
ALLOCATION_TEMPLATES = {**BASE_TEMPLATES, **EXTRA_TEMPLATES}

# Read separately from the persistence DB (this report's tracking helper table).
# {schema}/{table} come from config['persistence']; {min_yearweek} from the run.
LOST_ALLOC_SQL = """
    SELECT *
    FROM [{schema}].[{table}]
    WHERE YearWeek >= '{min_yearweek}'
"""
