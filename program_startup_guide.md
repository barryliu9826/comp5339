# Program Startup Guide

## Appendix: System Execution Commands

### A.1 Data Pipeline Execution

#### A.1.1 Full Pipeline Execution

**Command:**
```bash
python src/data/data_pipeline.py --mode full
```

#### A.1.2 Data Acquisition Only

**Command:**
```bash
python src/data/data_pipeline.py --mode acquisition
```

#### A.1.3 Data Transformation Only

**Command:**
```bash
python src/data/data_pipeline.py --mode transformation
```

#### A.1.4 Data Publication Only

**Command:**
```bash
python src/data/data_pipeline.py --mode publication
```

---

### A.2 Dashboard Application Execution

#### A.2.1 Monolithic Dashboard

**Command:**
```bash
python src/dashboard/monolithic_dashboard.py
```

**Access:**
- Web interface: `http://localhost:8050` (default Dash port)
- API endpoints: `http://localhost:8000` (default FastAPI port)
- WebSocket endpoint: `ws://localhost:8000/ws`

