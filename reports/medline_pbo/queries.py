"""SQL templates for the Medline PBO report.

Reuses the shared ``BASE_TEMPLATES`` (inventory / usage / PLM) and adds two
reads of the Daily Autosub load (reports/medline_autosub), which lives in the
SCSFileIngestor database on the same server:

- ``autosub_relation`` — each primary item's first-choice auto-substitute
  (``AutoSub Seq`` = 1) from MedlineAutosubRelation, which keeps every pair ever
  reported. A pair whose slot 1 was taken over by another sub that later dropped
  off the report is still at Seq 1, so a primary can have several; the one seen
  on the latest daily report wins (then the latest ``update stamp``).
- ``autosub_today`` — today's rows of the daily MedlineAutosub table, for each
  pair's current MIOH.
"""

from src.queries import BASE_TEMPLATES

EXTRA_TEMPLATES = {
    "autosub_relation": r"""
        SELECT [PRIMARY ML #], [AutoSub]
        FROM (
            SELECT r.[PRIMARY ML #], r.[AutoSub],
                   ROW_NUMBER() OVER (
                       PARTITION BY r.[PRIMARY ML #]
                       ORDER BY d.[Last Report Date] DESC, r.[update stamp] DESC
                   ) AS RK
            FROM SCSFileIngestor.MedlineAutoSub.MedlineAutosubRelation r
            LEFT JOIN (
                SELECT [PRIMARY ML #], [AutoSub], MAX([Report Date]) AS [Last Report Date]
                FROM SCSFileIngestor.MedlineAutoSub.MedlineAutosub
                GROUP BY [PRIMARY ML #], [AutoSub]
            ) d
              ON d.[PRIMARY ML #] = r.[PRIMARY ML #]
             AND d.[AutoSub] = r.[AutoSub]
            WHERE r.[AutoSub Seq] = 1
        ) a
        WHERE RK = 1
    """,
    "autosub_today": r"""
        SELECT [PRIMARY ML #], [AutoSub], [MIOH]
        FROM SCSFileIngestor.MedlineAutoSub.MedlineAutosub
        WHERE [Report Date] = CAST(GETDATE() AS DATE)
    """,
}

# Fetched in one fetch_tables() call (neither autosub read has a
# {location_filter}, so the format() pass leaves them unchanged).
PBO_TEMPLATES = {**BASE_TEMPLATES, **EXTRA_TEMPLATES}
