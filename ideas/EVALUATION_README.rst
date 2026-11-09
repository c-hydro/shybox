Model evaluation: design umbrella
=================================

Recorded: 2026-11-08.
Status: agreed direction for later design and implementation; no database,
schema, scoring thresholds or evaluation workflow has been implemented yet.

Purpose
-------

Build a reproducible system for evaluating Continuum/HMC and S3M using observed
and simulated time series, locating poor performance, investigating possible
causes, and validating corrections. Support both operational updates and
historical analysis. Hydrapp will display evaluation results.

The proposed package name is ``evaluation_toolkit``. Keep predictive evaluation
separate from ``monitoring_toolkit``: a successful execution does not establish
forecast accuracy. Keep these ideas and subsequent design decisions under this
``evaluation_ideas/`` umbrella until implementation is requested.

Processing scheme
-----------------

.. code-block:: text

   NetCDF/JSON simulations + independent observations
       -> normalize units, timestamps, identifiers and quality flags
       -> catalogue and time-series database
       -> pair predictions with observations
       -> metrics, uncertainty and versioned quality ratings
       -> evidence-based diagnostics and AI-assisted explanations
       -> candidate corrections
       -> independent validation
       -> published evaluation products and Hydrapp views

Storage approach
----------------

Start with PostgreSQL for relational metadata, numeric samples and evaluation
results. Consider PostGIS for spatial relationships and basin/location queries.
Consider TimescaleDB only after measuring ingestion and query requirements;
ordinary PostgreSQL partitioning provides an initial growth path.

Retain original NetCDF/GeoTIFF/JSON files as the authoritative source archive.
Record source paths, checksums and import provenance. Initially store extracted
station, river-section and basin statistics rather than every raster cell.

Conceptual entities (names remain provisional):

* ``locations``: stable identities for river sections, snow stations and regions;
  coordinates, basin and elevation.
* ``location_versions``: effective-dated changes to thresholds, drainage area,
  station metadata and observation methods.
* ``variables``: discharge, SWE, snow depth, rainfall, temperature and other
  variables; units, spatial support and temporal/aggregation meaning.
* ``model_runs``: model/version, initialization, forcing source, configuration
  hash, assimilation settings and execution reference.
* ``series``: location, variable, model run or observation source, ensemble member
  and product type.
* ``samples``: valid time, numeric value, quality flags and revision.
* ``evaluations``: period/event, lead-time group, pairing rules, metrics, sample
  coverage, uncertainty and scoring-profile version.
* ``diagnostics``: detected error patterns, evidence and proposed explanations.
* ``corrections``: method/version, training period, parameters, source products
  and independent validation results.

Important data rules
--------------------

* Distinguish model initialization, sample validity and information-availability
  time. Retain ingestion time and observation revisions so historical analyses
  can reconstruct what was known at forecast issue time.
* Parse the existing comma-separated JSON series into numeric samples. Convert
  missing sentinels to database NULL values and retain quality flags.
* Preserve raw records and provenance. Make re-imports idempotent; do not
  overwrite history or corrected products silently.
* Preserve all forecasts for the same valid time with their initialization,
  lead time and member. An operational latest-forecast view must not replace
  the historical forecast archive.
* Keep assimilated analysis, open-loop predictions and corrected predictions
  distinguishable. Record warm-up exclusions and observation eligibility.
* Match units, time intervals and spatial representativeness before scoring.
  Missing or questionable observations must not automatically imply a bad model.

Evaluation and quality scale
----------------------------

For Continuum, initially evaluate bias, MAE/RMSE, NSE/KGE, event volume, peak
magnitude, peak timing and threshold-exceedance detection. Stratify by section,
season, forcing source and forecast lead time. Compare with simple reference
forecasts, such as persistence or an appropriate seasonal baseline.

For S3M, add SWE, snow depth, snow-cover extent and accumulation/melt timing,
using observations with suitable spatial representativeness. Discharge alone
cannot fully evaluate snow-model behaviour.

For ensembles, assess the distribution as well as members: CRPS, reliability
and threshold probabilities are candidate measures.

Publish a metric vector first. A subsequent configurable 0--100 score and A--E
class must have a versioned scoring profile and retain the underlying metrics,
coverage and uncertainty. Include an explicit insufficient-evidence state.
Use different profiles for flood forecasting, ordinary discharge and seasonal
snow evaluation. Do not impose universal thresholds or arbitrary weights now,
or convert raw NSE/KGE directly to percentages.

AI diagnostics and corrections
------------------------------

Time series locate where and when errors occur. Investigating why also needs
forcing errors, initial states, assimilation, snow conditions, basin properties,
reservoir operations and observation-quality information.

AI explanations should cite the supporting runs, events and measurements and
distinguish associations from established causes. Start with transparent metrics
and diagnostic rules before training explanatory or correction models.

Preserve original simulations. Publish corrections as separately versioned
products, with their training data and method recorded. Keep forecast
post-processing separate from changes to physical-model parameters or states.
Validate chronologically or by held-out events, training strictly before the
evaluation period. Compare corrected and raw predictions against the same
observations and baseline. Avoid random splits that leak adjacent timesteps or
the same event between training and evaluation.

Operational and historical workflow
-----------------------------------

Use the same ingestion, matching and evaluation pipeline in both modes:

* Real time: ingest new runs and observations, produce provisional evaluations,
  and revise them reproducibly as eligible observations arrive or change.
* History: select explicit date ranges, model/configuration versions and
  observation revisions, then reproduce evaluations and diagnostics.

Proposed entry points, to be created later:

.. code-block:: text

   shybox/evaluation_toolkit/
   workflow/evaluation/app_evaluation_main.py
   workflow/evaluation/evaluation.example.json

Configuration should declare input variable mappings, observation sources,
database connection via environment variables, quality rules, warm-up exclusion,
evaluation windows, lead-time groups, baselines and versioned scoring profiles.

Implementation sequence
-----------------------

1. One basin, deterministic discharge, source catalogue and PostgreSQL ingestion.
2. Observation matching, data-quality checks and transparent metric reports.
3. Real-time/historical operation and Hydrapp evaluation views.
4. Snow-specific S3M evaluation and ensemble verification.
5. Evidence-based AI diagnostics.
6. Candidate corrections and independent validation.

Decisions still needed
----------------------

Before implementation, establish observation sources and their quality flags,
available independent snow measurements, historical depth, sample cadence,
ensemble size, expected data volume, database deployment and retention needs.
These determine indexes, partitioning and whether extensions are worthwhile.
Scoring thresholds and correction methods require a representative baseline
dataset and an agreed operational objective.

References
----------

* `PostgreSQL table partitioning <https://www.postgresql.org/docs/current/ddl-partitioning.html>`_
* `PostGIS documentation <https://postgis.net/docs/en/>`_
* `HEC-HMS calibration summary statistics <https://www.hec.usace.army.mil/confluence/hmsdocs/hmstrm/calibration/calibration-summary-statistics>`_
* `ECMWF ensemble verification <https://confluence.ecmwf.int/download/attachments/72127032/v2handout.pdf?api=v2>`_
