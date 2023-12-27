# query-learn

## Postgres


### Start Server
```shell
docker run --name postgres -p 5432 -e POSTGRES_PASSWORD=secret postgres
```

### Start Pgsql
```shell
$ docker exec -ti postgres psql -h localhost -U postgres 

create database tpch;
```

### Import TPCH data
```shell
(cd scripts; pip3 install -r requirements.txt; python3 setup-postgres.py) 
```

### Use data
```shell
$ docker exec -ti postgres psql -h localhost -U postgres 

\c tpch;
select * from nation;

SELECT n."N_NAME", SUM(s."S_ACCTBAL") AS total_acctbal
FROM nation n
JOIN supplier s ON n."N_NATIONKEY" = s."S_NATIONKEY"
GROUP BY n."N_NAME"
ORDER BY n."N_NAME";
```
