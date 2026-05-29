# Churn Prediction for Dynamics 365 CRM

End-to-end machine learning pipeline that predicts account churn in Dynamics 365 using a PyTorch MLP, serves predictions via FastAPI, and triggers proactive retention actions through Power Automate.

## What this does

When a sales rep updates an account in Dynamics 365, a Power Automate flow calls a FastAPI endpoint hosted on Azure. The endpoint runs a trained neural network on the account's signals (open cases, recent activities, pipeline value, days since last opportunity) and returns a churn probability. If the risk is high, a follow-up task is automatically created in D365 Sales for the rep — surfaced on their mobile app. Sales outcomes then flow back to Dataverse and feed monthly retraining.

## Architecture
## Tech stack

| Layer       | Tools                                            |
|-------------|--------------------------------------------------|
| ML          | PyTorch (MLP), scikit-learn (baselines), MLflow  |
| API         | FastAPI, Pydantic                                |
| Data access | Dataverse Web API, MSAL OAuth                    |
| Cloud       | Azure App Service                                |
| Automation  | Power Automate, Power BI                         |

## Quick start

```bash
git clone https://github.com/rafaeder-maiorino/churn-d365.git
cd churn-d365
python3.11 -m venv .venv
source .venv/bin/activate
make install
```

## Make targets

| Command         | What it does                              |
|-----------------|-------------------------------------------|
| `make install`  | Install dependencies (editable)           |
| `make lint`     | Run ruff                                  |
| `make format`   | Auto-format with ruff                     |
| `make test`     | Run pytest                                |
| `make run-api`  | Start FastAPI locally on `:8000`          |
| `make eda`      | Open Jupyter Lab                          |

## Project structure
churn-d365/
├── src/
│   ├── api/              # FastAPI app
│   ├── monitoring/       # drift detection
│   └── ...               # preprocessing, MLP, training
├── data/                 # raw and processed datasets (gitignored)
├── models/               # serialized artifacts (gitignored)
├── notebooks/            # EDA and experimentation
├── tests/                # pytest suite
├── power_automate/       # exported flow definitions
├── power_bi/             # PBIX files and templates
├── infra/                # Azure deployment scripts
├── docs/                 # ML Canvas, Model Card, architecture notes
├── pyproject.toml
├── Makefile
└── README.md
## Roadmap

- [x] **Block 1** — Environment setup and project scaffolding
- [ ] **Block 2** — Dataverse Web API client (OAuth via App Registration)
- [ ] **Block 3** — Exploratory data analysis and ML Canvas
- [ ] **Block 4** — Baselines + MLP training with MLflow tracking
- [ ] **Block 5** — FastAPI inference service + tests
- [ ] **Block 6** — Azure deployment + Power Automate flow + Power BI dashboard
- [ ] **Block 7** — Model Card, monitoring plan, and YouTube walkthrough

## Why this project?

Most ML tutorials stop at "model.pkl saved". This one connects the dots all the way to the business: live data from a CRM, a real automation that creates actionable tasks for sales, and a feedback loop that retrains on actual outcomes. The goal is to show what production ML inside a Microsoft Power Platform shop actually looks like — and to document it openly as a reference for others working at the same intersection.

## Author

[Rafael Maiorino](https://github.com/rafaeder-maiorino) — building this as a follow-up to the [FIAP MLE Tech Challenge](https://postech.fiap.com.br/).
