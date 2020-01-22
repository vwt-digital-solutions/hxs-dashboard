# HXS Dashboard

This repository contains the dashboard for Hybrid Access project related data, build with Dash.

## Python environment setup

```
export VENV=~/env
conda create --prefix $VENV python=3.7
conda activate $VENV
pip install -r requirements.txt
```

## Run application

```
python index.py
```

In GAE with gunicorn:

```
gunicorn -b :$PORT index:app.server --timeout 360 --workers 3
```

## Setting google cloud credentials

https://cloud.google.com/docs/authentication/getting-started
