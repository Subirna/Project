#!/bin/bash
echo "=== Installing Python dependencies ==="
PYTHON=python3

echo "Python:  $($PYTHON --version 2>&1)"
echo "At:      $(which $PYTHON)"
echo "Pip:     $($PYTHON -m pip --version 2>&1 | head -1)"

# IMPORTANT: do NOT use --user.
# python3 here is the pyenv36 Python (/home/consultant/pyenv36/bin/python3).
# When pyenv is active, ~/.local/lib/... (user site) is excluded from sys.path,
# so --user installs are invisible at runtime.  Installing without --user puts
# packages into /home/consultant/pyenv36/lib/python3.6/site-packages/ which
# IS on sys.path, so imports work.

# Step 1: Upgrade setuptools inside pyenv (needed for thrift's setup.py to run).
echo "--- Upgrading setuptools in pyenv ---"
$PYTHON -m pip install "setuptools>=40,<60" 2>&1 | tail -3

# Step 2: Pure-Python packages — no C build needed.
echo "--- Installing requests and kafka-python ---"
$PYTHON -m pip install requests kafka-python 2>&1 | tail -3

# Step 3: thrift — requires C build. Try latest first, fall back to older
# versions known to support Python 3.6.
echo "--- Installing thrift ---"
$PYTHON -m pip install thrift 2>&1 | tail -3 || \
$PYTHON -m pip install "thrift==0.15.0" 2>&1 | tail -3 || \
$PYTHON -m pip install "thrift==0.13.0" 2>&1 | tail -3 || \
$PYTHON -m pip install "thrift==0.11.0" 2>&1 | tail -3 || \
echo "WARNING: all thrift versions failed"

# Step 4: happybase without deps (skips thriftpy2 which also needs C build).
# At runtime happybase falls back to thrift if thriftpy2 is absent.
echo "--- Installing happybase ---"
$PYTHON -m pip install --no-deps happybase 2>&1 | tail -3

# Step 5: Verify every import works end-to-end.
echo "--- Verifying imports ---"
$PYTHON -c "import requests; print('requests', requests.__version__)"
$PYTHON -c "import kafka; print('kafka-python OK')"
$PYTHON -c "import thrift; print('thrift OK')" || echo "WARNING: thrift import failed"
$PYTHON -c "import happybase; print('happybase OK')" || echo "WARNING: happybase import failed"

echo "=== Dependency installation complete ==="
