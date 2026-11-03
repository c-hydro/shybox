# 🌨️ SHYBOX – Snow and Hydrologic Modeling Toolbox

[![License](https://img.shields.io/badge/license-EUPL--1.2-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/environment-Python%203.12-blue)](https://www.python.org/)
[![Release](https://img.shields.io/github/v/release/c-hydro/shybox)](https://github.com/c-hydro/shybox/releases)

**SHYBOX** (Snow and HYdrologic toolBOX) is a modular hydrological processing framework designed to run **reproducible workflows** using **versioned environmental datasets**.

It is developed and maintained by the **CIMA Research Foundation** and is used both for **operational forecasting systems** and **research activities** in hydrology and cryosphere modeling.

SHYBOX is intended to work together with the companion dataset repository  
👉 **[`shydata`](https://github.com/c-hydro/shydata)**

---

## 🔎 Overview

SHYBOX provides tools and workflows to process hydrological and environmental data in a **structured, configurable, and reproducible** way.

Key design principles:

- Separation between **processing logic** and **data distribution**
- Modular processing chains
- Traceability and reproducibility of experiments
- Clean integration with versioned datasets

⚠️ **Datasets are not included in this repository** and must be retrieved separately from `shydata`.

---

## 🎯 Objectives

The main objectives of SHYBOX are to:

- Support reproducible hydrological and snow-modeling workflows
- Provide modular and configurable processing chains
- Integrate Python and Fortran-based modeling engines
- Operate consistently across research and operational environments

---

## 📦 Features

- Modular design for snow and hydrological modeling
- Integration with operational systems (e.g. Flood-PROOFS)
- Support for Python and Fortran-based models:
  - **S3M** – Distributed Snow Simulation System  
  - **HMC** – Hydrological Modeling Chain
- Tools for:
  - Data ingestion and transformation
  - Multi-source time-series processing
  - Spatial interpolation and masking
  - 2D/3D visualization of hydrometeorological fields

---

## 📂 Repository Structure

```text
shybox/
├── shybox/             # Core Python package
├── workflow/           # Workflow drivers and JSON examples
├── exec/               # Case-study execution directories
├── apps/               # Additional applications
├── web/                # Result visualization interface
├── docker/             # Container definitions
├── apptainer/          # Apptainer deployment notes
├── hpc/                # HPC execution material
├── utils/              # Standalone utilities
├── docs/               # Documentation and examples
├── tests/              # Tests
└── README.md
```

---

## 🗃️ Datasets – `shydata`

SHYBOX relies on datasets provided by the companion repository:

🔗 **https://github.com/c-hydro/shydata**

- Datasets are distributed via **versioned releases**
- Data must be retrieved and stored locally before running workflows
- Dataset structure and conventions are documented in `shydata`

---

## 🚀 Installation

```bash
git clone https://github.com/c-hydro/shybox.git
cd shybox
```

Environment setup (Conda recommended):

```bash
bash setup_shybox_base.sh
source ./conda/shybox_base_settings
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
```

---

## ▶️ Usage

SHYBOX workflows are driven by **configuration files** and executed via the provided scripts and tools.

Refer to:
- [SHYBOX documentation](docs/index.rst) for installation, configuration, workflows, and development
- Configuration examples for model-specific setups (S3M, HMC)

The first documentation edition is **0.1.0**, independent of the package version.
Build its HTML with Python 3.12 and the dependencies in `docs/requirements.txt`;
see [documentation development](docs/development.rst) and
[Read the Docs publishing](docs/publishing.rst).

---

## 📜 License

This project is licensed under the  
**European Union Public License v1.2 (EUPL-1.2)**

---

## 🔗 Related Repositories

- **SHYBOX (processing framework)**  
  https://github.com/c-hydro/shybox

- **SHYDATA (datasets)**  
  https://github.com/c-hydro/shydata
