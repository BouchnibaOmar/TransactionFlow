@echo off
setlocal
set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot"
set "PATH=%JAVA_HOME%\bin;%PATH%"
set "PYSPARK_PYTHON=%~dp0.venv\Scripts\python.exe"
set "PYSPARK_DRIVER_PYTHON=%PYSPARK_PYTHON%"
cd /d "%~dp0"
"%PYSPARK_PYTHON%" %*
exit /b %errorlevel%
