# Prototyping notebooks — reference only

These Jupyter notebooks are the **original prototypes** used to work out the
report logic interactively:

- `Medline PBO.ipynb` — prototype for the Medline PBO report.
- `MedlineAllocation.ipynb` — prototype for the Medline Allocation report.

**They are not the production code.** The production pipelines live in:

- `src/` — shared infrastructure (config, secrets, logging, email, db, styler, runner)
- `reports/medline_pbo/` and `reports/medline_allocation/` — report-specific steps
- `run_medline_pbo.py` and `run_medline_allocation.py` — entry points

The notebooks are kept here for reference and future experimentation only. The
production code has been refactored and **may diverge** from these notebooks as
it changes over time — always treat `src/` + `reports/` as the source of truth.

Notes:
- These `.ipynb` files are git-ignored (see `.gitignore`) and are **not published**.
- Credentials have been redacted from the notebooks. Do not paste real secrets
  here; production loads them from the encrypted `.env` (see
  `src/config_loader.load_secrets`).
