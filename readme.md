# Assignment 02 – Realtime Energy Dashboard (Backend + Frontend + Data Tools)

This repository contains a production-oriented project that ingests energy data, publishes MQTT streams, serves a FastAPI backend with WebSocket broadcasting, and renders a Dash-based frontend for realtime visualization.

- Backend: `src/dashboard/backend.py` (FastAPI + WebSockets)
- Frontend: `src/dashboard/frontend.py` (Dash + Plotly + Leaflet + WebSockets client)
- Shared Schemas: `src/dashboard/schemas.py` (Pydantic v2 models)
- Data acquisition & conversion: `src/data/` (OpenElectricity SDK integration, CSV/JSON tools)
- MQTT publisher: `src/data/mqtt_publisher.py`


## 1) Requirements

- Python 3.12+
- Pip (or uv/poetry if you prefer)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Key dependencies:
- FastAPI, Uvicorn, WebSockets
- Dash, Plotly, dash-leaflet
- pandas
- paho-mqtt
- pydantic>=2
- openelectricity (OpenElectricity SDK)


## 2) Configuration

Configuration primarily uses environment variables. Create a `.env` file in the repo root or export the variables in your shell.

Recommended variables:

- OPENELECTRICITY_API_KEY: API key for OpenElectricity
- OE_BASE_URL: Optional custom API base URL
- INTERVAL: Data interval for acquisition (e.g. `5m`)
- BATCH_SIZE: Batch size for acquisition (default `30`)
- BATCH_DELAY_SECS: Delay between batches in seconds (default `0.5`)
- NETWORK_FILTER: Network filter (default `NEM`)
- STATUS_FILTER: Status filter (default `operating`)

Example `.env`:

```bash
OPENELECTRICITY_API_KEY=your_api_key_here
OE_BASE_URL=
INTERVAL=5m
BATCH_SIZE=30
BATCH_DELAY_SECS=0.5
NETWORK_FILTER=NEM
STATUS_FILTER=operating
```


## 3) Project Structure

```
/assignment02_1107
  ├─ src/
  │  ├─ dashboard/
  │  │  ├─ backend.py        # FastAPI backend with WebSocket broadcasting
  │  │  ├─ frontend.py       # Dash app (web UI) consuming WS updates
  │  │  └─ schemas.py        # Pydantic v2 models
  │  └─ data/
  │     ├─ data_acquisition.py       # Pulls data via OpenElectricity SDK
  │     ├─ json_to_csv_converter.py  # Converts project JSON to CSV outputs
  │     └─ mqtt_publisher.py         # Publishes facility/market data to MQTT
  ├─ data/
  │  ├─ batch_data/                  # Raw/processed batches (JSON)
  │  ├─ csv_output/                  # Generated CSVs
  │  ├─ facilities_data.json
  │  ├─ facility_metrics.json
  │  ├─ facility_unit_mapping.json
  │  ├─ facility_units_metrics.json
  │  └─ market_data.json
  ├─ logs/                           # Runtime logs
  ├─ start_all.sh / stop_all.sh      # Linux/macOS helpers
  ├─ start_all.bat / stop_all.bat    # Windows helpers
  └─ requirements.txt
```


## 4) Running the System

You can start components individually or use the helper scripts.

### Option A: Start Everything with Helper Scripts

macOS/Linux:
```bash
bash start_all.sh
# to stop
bash stop_all.sh
```

Windows:
```bat
start_all.bat
:: to stop
stop_all.bat
```

These scripts will, in order: run the backend, run the frontend, and (optionally, if configured) run the MQTT publisher.


### Option B: Run Components Manually

#### 4.1 Backend (FastAPI)

Runs a WebSocket server to push facility and market updates to clients.

```bash
python -m uvicorn src.dashboard.backend:app --host 0.0.0.0 --port 8000 --reload
```

- Health: `http://localhost:8000`
- WebSocket endpoint used by the frontend is defined inside `backend.py`.


#### 4.2 Frontend (Dash)

Dash web UI that subscribes to the backend WebSocket and renders maps, charts, and controls.

```bash
python src/dashboard/frontend.py
```

Default URL (printed on startup), usually `http://127.0.0.1:8050`.


#### 4.3 MQTT Publisher

Publishes both facility and market streams to the configured MQTT broker, reading from CSV outputs.

- Defaults:
  - Facility CSV: `data/csv_output/facility_metrics_wide.csv`
  - Market CSV: `data/csv_output/market_metrics_data.csv`
  - Facility Topic: `a02/facility_metrics/v1/stream`
  - Market Topic: `a02/market_metrics/v1/stream`
  - Broker: `broker.hivemq.com:1883`
  - QoS: `0`

Run with defaults:
```bash
python src/data/mqtt_publisher.py
```

Custom paths and broker:
```bash
python src/data/mqtt_publisher.py \
  --facility-csv data/csv_output/facility_metrics_wide.csv \
  --market-csv data/csv_output/market_metrics_data.csv \
  --broker-host broker.hivemq.com \
  --broker-port 1883 \
  --facility-topic a02/facility_metrics/v1/stream \
  --market-topic a02/market_metrics/v1/stream \
  --qos 0
```

Help:
```bash
python src/data/mqtt_publisher.py --help
```


## 5) Data Acquisition & Conversion

### 5.1 Acquire Data via OpenElectricity SDK

`src/data/data_acquisition.py` integrates the OpenElectricity SDK to fetch facilities, unit time-series, and market metrics, then aggregates facility-level outputs.

- Configure with `.env` vars (see Configuration)
- Output targets are under `data/` JSON files and/or to `data/csv_output/` via downstream conversion

Run:
```bash
python src/data/data_acquisition.py
```

### 5.2 Convert JSON to CSV

`src/data/json_to_csv_converter.py` converts project JSON files into normalized CSVs under `data/csv_output/` for downstream analytics and MQTT streaming.

Run:
```bash
python src/data/json_to_csv_converter.py \
  --input-dir data \
  --output-dir data/csv_output
```


## 6) Logging

Runtime logs are written to `logs/`:
- `backend.log`, `frontend.log`, `mqtt_publisher.log`, `data_acquisition.log`, etc.

Ensure the directory exists (created automatically by most components). Review these logs when debugging.


## 7) Notes & Troubleshooting

- Ensure your `OPENELECTRICITY_API_KEY` is valid before running acquisition.
- Backend and frontend both rely on WebSockets; ensure ports are open and not in use.
- If CSV files are large (e.g., `facility_metrics_wide.csv`), operations may take time. Use filters or smaller windows if needed.
- MQTT broker defaults to a public broker (`broker.hivemq.com`) for convenience. Use a private broker for production.


## 8) License & Credits

This project is for academic purposes (COMP5339 Assignment 02). Data acquisition leverages the OpenElectricity SDK and public energy market data where applicable. Plotting by Plotly; mapping by dash-leaflet.
