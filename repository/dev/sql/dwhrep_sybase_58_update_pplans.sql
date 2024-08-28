update partitionplan
set DEFAULTSTORAGETIME = 10000000000,
MAXSTORAGETIME = 10000000000
where partitionplan = 'sgehextralarge_raw' and DEFAULTSTORAGETIME = 8243272800
