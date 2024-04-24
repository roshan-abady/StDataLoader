#!/usr/bin/env python3
import os
from streamlit.web import cli

if __name__ == "__main__":
    # Get the absolute path to the main directory
    main_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the absolute path to app.py
    app_path = os.path.join(main_dir, "app.py")
    
    cli.main_run([app_path])