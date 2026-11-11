# Dataset planning and workflow setup

`app_runner_workflow_hmc_datasets_main.py` plans observed/forecast files, checks steps -3 through +1, fills a trailing observed gap from complete forecasts and configures the per-variable processing workflows.

Use the deterministic HMC, probabilistic HMC or S3M settings in this folder. Resolve the environment roots (`DOMAIN_NAME`, `PATH_GEO`, `PATH_OBS`, `PATH_FRC_NWP`, `PATH_DST`, `PATH_LOG`; also `PATH_FRC_RFARM` for rainfall ensembles) and run from the repository root:

```bash
python -m workflow.runner.datasets.app_runner_workflow_hmc_datasets_main \
  -settings_file workflow/runner/datasets/app_runner_workflow_hmc_datasets_forcing_deterministic.json \
  -time "2026-11-09 09:35" --dry-run --plan-files
```

Source entries declare `dims: "2d"` or `"3d"`, input-to-canonical `variable` mappings and deterministic/probabilistic mode. Destination collections declare canonical-to-output `variable` mappings and model units. The ensemble profile explicitly puts outputs under `run_{ensemble_member}`.

The top-level `workflow.process_list` contains `RAIN_WORKFLOW`, `AIR_T_WORKFLOW`, `INC_RAD_WORKFLOW`, `REL_HUM_WORKFLOW` and `WIND_SPEED_WORKFLOW`. The sample chains pass values through with `mask_data_by_limits`; configure the needed interpolation, masking and unit conversion explicitly.

`configure_dataset_workflows(planner, configuration)` creates one source/destination `DataLocal` pair for every planned variable and one combined `OrchestratorConfig` per member/time, following the NWP converter pattern. It returns `{member: {time: {"source": handlers, "destination": handlers, "orchestrator": obj}}}`. Source paths, output paths and member selection use the adjusted planner. Setup does not read arrays or write forcing; run the returned orchestrators explicitly when inputs and process chains are ready.

See [dataset planning](../../../docs/dataset_planning.rst) and [model execution](../../../docs/runners.rst) for complete configuration and the handoff to HMC/S3M runners.
