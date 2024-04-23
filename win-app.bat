@echo off
for /f "delims=" %%i in ('where streamlit') do set streamlitpath=%%i
%streamlitpath% run .\main\app.py