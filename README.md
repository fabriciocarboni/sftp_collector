# sftp_collector

This program was created to collect a huge amount of gz files from several differents servers in a Telecom company.

Author: Fabricio Carboni

sftpcollector.py - It uses multprocessing module to parallelize sftp get on the servers.

sftpcollector_probe.py - It's a probe just to show how's the collect process is going on. It shows:

1. The amount of files being collect at the moment on each server
1. The amount of files collected 