# sftp_collector

This program has been created to collect a huge amount of gz files from several differents servers in a Telecom company.

Author: Fabricio Carboni

**sftpcollector.py** - It uses multprocessing module to parallelize sftp get on the servers.

**sftpcollector_probe.py** - It's a probe just to show how's the collect process is going on. It shows:

1. Amount of files being collected at the moment on each server
1. Amount of files collected on the day
1. Total size in GB / TB on the day
1. How many disk space is available on the local server
1. Amount of files not moved to HDFS
1. Load average of the local server
