# Folder for installing and managing influxdb

### Setup
1. run ```docker-compose```
2. Go to ```localhost:8086```
3. Provide
    Username: ```admin```
    Password: ```adminadmin```
4. Go to Explore
5. Click on the Script editor button
6. Copy paste the following query:
```
from(bucket: "ost_sm")
  |> range(start: time(v: "2009-07-01"), stop: time(v: "2010-07-01"))
  |> filter(fn: (r) => r["_measurement"] == "Measurement")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "mean")
```
7. Submit

### To-test if you receive data
1. Run ```python3 src/test.py``` from host (outside of docker)
2. Reload the dashboard