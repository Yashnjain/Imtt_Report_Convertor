SET logfile="C:\Users\Yashn.jain\Documents\power_automate\imtt_report_converter\logs\batch.log"
@echo off@echo Starting Script at %date% %time% >> %logfile%
"C:\Program Files\Python38\python.exe" "C:\Users\Yashn.jain\Documents\power_automate\imtt_report_converter\imtt_report_converter.py"
@echo finished at %date% %time% >> %logfile%