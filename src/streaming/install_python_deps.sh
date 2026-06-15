#!/bin/bash
echo "=== Installing Python dependencies ==="
PYTHON=python3

echo "Python: $($PYTHON --version 2>&1)"

# Step 1: Upgrade setuptools + wheel (both have pre-built wheels so pip 9.0.3 can handle them).
# Newer setuptools is what allows pip 9.0.3 to successfully run 'setup.py egg_info'
# when building thrift from source.
echo "--- Upgrading setuptools and wheel ---"
$PYTHON -m pip install --user "setuptools>=40.0,<60.0" wheel 2>&1 | tail -3

# Step 2: Pure-Python packages — no C build needed, always succeed.
echo "--- Installing requests and kafka-python ---"
$PYTHON -m pip install --user requests kafka-python 2>&1 | tail -3

# Step 3: thrift — needs C build.  Try latest first, fall back to older releases
# that are known to support Python 3.6 with a simple setup.py.
echo "--- Installing thrift ---"
$PYTHON -m pip install --user thrift 2>&1 | tail -3 || \
$PYTHON -m pip install --user "thrift==0.15.0" 2>&1 | tail -3 || \
$PYTHON -m pip install --user "thrift==0.13.0" 2>&1 | tail -3 || \
$PYTHON -m pip install --user "thrift==0.11.0" 2>&1 | tail -3 || \
echo "WARNING: all thrift versions failed"

# Step 4: happybase — install without deps to skip thriftpy2.
# At runtime happybase tries thriftpy2 first, falls back to thrift, which we just installed.
echo "--- Installing happybase ---"
$PYTHON -m pip install --user --no-deps happybase 2>&1 | tail -3

# Step 5: Verify imports work end-to-end.
echo "--- Verifying imports ---"
$PYTHON -c "import requests; print('requests', requests.__version__)"
$PYTHON -c "import kafka; print('kafka-python OK')"
$PYTHON -c "import thrift; print('thrift OK')" || echo "WARNING: thrift import failed"
$PYTHON -c "import happybase; print('happybase OK')" || echo "WARNING: happybase import failed"

echo "=== Dependency installation complete ==="
