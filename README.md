# DNR1 Wave Plan Tool v1.3

A web-based wave planning tool for DNR1 Yard Marshal operations.

## Features

- **Wave Plan Generation**: Build wave plans from dispatch, assignment, and SCC data
- **Lane Optimization**: Cart-aware lane pairing (max 6 carts/lane)
- **DSP Slack Integration**: Send wave alerts to DSP-specific OPS channels
- **Excel Export**: Download print-optimized wave plan spreadsheets
- **Real-time Status**: Track wave clearing and dispatch status

## Quick Start

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
python app.py
# or
start_server.bat
```

Visit: http://localhost:5001

### Data Sources Required

1. **Dispatch Plan** - Wave timing and route assignments
2. **Assignment Planning** - Driver/DSP assignments
3. **SCC Pick Export** - Cart counts per route
4. **PickOrder CSV** (optional) - Lane spreading

## Deployment

### Render (Recommended)

1. Push to GitHub
2. Connect repo to Render
3. Use `render.yaml` for auto-configuration
4. Environment variables are auto-generated

### Manual

```bash
gunicorn app:app --bind 0.0.0.0:8080
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main dashboard |
| `/export/wave-plan` | GET | Download Excel file |
| `/api/plan` | GET | Get wave plan JSON |
| `/api/slack/send-wave/<n>` | POST | Send wave n to all DSPs |
| `/api/slack/send-all` | POST | Send all waves to all DSPs |

## Configuration

### Environment Variables

- `SECRET_KEY` - Flask session key (auto-generated on Render)
- `FLASK_DEBUG` - Enable debug mode (default: false)
- `PORT` - Server port (default: 5001)

### DSP Webhooks

Configure in `slack_client.py`:
- `DSP_OPS_WEBHOOKS` - OPS channel webhooks per DSP
- `DSP_METRICS_WEBHOOKS` - Metrics channel webhooks per DSP

## File Structure

```
dnr1-wave-plan/
├── app.py                  # Flask application
├── wave_engine.py          # Wave plan builder
├── lane_optimizer.py       # Cart-aware lane pairing
├── slack_client.py         # DSP Slack integration
├── wave_plan_excel_generator.py  # Excel export
├── data_manager.py         # Data source management
├── scc_parser.py           # SCC CSV parser
├── pickorder_parser.py     # PickOrder CSV parser
├── templates/
│   └── am_view.html        # Main dashboard
├── render.yaml             # Render deployment config
└── requirements.txt        # Python dependencies
```

## Version History

- **v1.3** - Excel export with print optimization
- **v1.2** - DSP Slack integration with 13 webhooks
- **v1.1** - Lane optimizer with cart pairing
- **v1.0** - Initial wave plan builder

---
DNR1 Operations | Built with ❤️ for Yard Marshals
