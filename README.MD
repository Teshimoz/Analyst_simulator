# Analyst Simulator

There are some files I created as a part of the course and put on CI/CD schedule:
* `large_report_plots_gt.py` - sript to generate daily report: values, changes and plots of key product metrics
* `report_anomaly_gt.py ` - script to send alert if our key metric change out of bounadries (allowed deviation corridor defined by history data)

Used: telegram bot to send text and pictures, matplotlib+seaborn to visualize the data, pandas to work with data, pandahouse to get the data from the clickhouse database

**The data generated as a part of Analyst Simulator from Karpov Cources,
avaliable for students on server*
