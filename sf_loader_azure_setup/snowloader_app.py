#!/usr/bin/env python3
# snowloader_app.py 
import subprocess
import sys

# Path to the Streamlit script
streamlit_script_path = 'scripts/snowloader.py'

# Launches the Streamlit app
subprocess.run(['streamlit', 'run', streamlit_script_path] + sys.argv[1:])
