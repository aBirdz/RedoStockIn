#!/bin/bash
cd /opt/redoStockIn/
unzip instantclient-basic-linux.x64-11.2.0.4.0.zip
cd instantclient_11_2/
cp * /usr/lib64/
cd /opt/redoStockIn/
rpm -ivh cx_Oracle-5.2.1-11g-py26-1.x86_64.rpm
cd /usr/lib64/
ln -s libclntsh.so.11.1 libclnts.so