# 🌨️ shybox: Snow and Hydrologic Modeling Toolbox

[![License](https://img.shields.io/badge/license-EUPL--1.2-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![Release](https://img.shields.io/github/v/release/c-hydro/shybox)](https://github.com/c-hydro/shybox/releases)

**shybox** (Snow and HYdrologic toolBOX) is a Python- and Fortran-based toolkit designed to support operational and research-level hydrological modeling. It is developed and maintained by the [CIMA Research Foundation](https://www.cimafoundation.org/), with active use in Italy’s regional civil protection forecasting systems.

---

## 📦 Features

- Modular design for snow and hydrological modeling
- Integration with Flood-PROOFS system
- Support for Python and Fortran-based modeling engines (e.g., S3M, HMC)
- Tools for:
  - Data ingestion and transformation
  - Multi-source time-series analysis
  - 2D/3D visualization of hydrometeorological fields
  - Laboratory simulation environments for research & education

---

## 🚀 Getting Started

### Prerequisites

- Linux (Debian/Ubuntu preferred)
- Python ≥ 3.8
- Fortran compiler (e.g., `gfortran`)
- Git, Conda (recommended)

### Installation

```bash
# Clone the repository
git clone https://github.com/c-hydro/shybox.git
cd shybox

# Set up the environment (Conda recommended)
bash setup/setup_conda_shybox_base.sh
```

---

## 📂 Repository Structure

```text
├── bin/                # Compiled model executables
├── configuration/      # JSON settings for workflows
├── datasets/           # Sample or default input data
├── script/             # Python driver scripts
├── setup/              # Environment and dependency setup scripts
├── lib/                # Python modules for hydrology (hyde)
├── docs/               # Documentation and usage examples
└── README.md
```

---

## 📊 Models Supported

- **S3M** – Distributed Snow Simulation System
- **HMC** – Hydrological Modeling Chain

> Each model may require its own build process and configuration.

---

## 🧪 Use Cases

- Regional flood forecasting
- Snowpack modeling
- Civil protection alert systems
- Research and education on hydro-meteorological processes

---

## 📝 License

This project is licensed under the **EUPL-1.2** – see the [LICENSE](LICENSE) file for details.

---

## 🤝 Contributing

Contributions, bug reports, and suggestions are welcome! To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature-name`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature-name`)
5. Create a pull request

---

## 📬 Contact

For questions, collaborations or support, reach out via [CIMA Foundation](https://www.cimafoundation.org/) or open an issue on [GitHub](https://github.com/c-hydro/shybox/issues).

---

## 🌐 Acknowledgements

- CIMA Research Foundation
- Italian Civil Protection Department
- Contributors to Flood-PROOFS and the hydrological modeling community
