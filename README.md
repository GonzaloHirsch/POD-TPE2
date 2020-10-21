# TPE2 - POD

## How to download dataset
In order to download the datasets, it can be done with SCP:
```
scp USERNAME@pampero.itba.edu.ar:/afs/it.itba.edu.ar/pub/pod/FILE.csv LOCAL_PATH
```
Where LOCAL_PATH is the path to where the file will be downloaded to (it can be moved later)

Files can be placed in an **examples** directory in the root of the project.

## Building
To build the project just run:
```
mvn clean install
```

## Running
At least two(2) terminals are going to be needed. These terminals are noted A, B, etc.

**NOTE**: This commands are meant to be run from the root of the project.

### Hazelcast Cluster
From terminal A run:
```
cd server/target/
tar -xzf tpe2-g2-server-1.0-SNAPSHOT-bin.tar.gz
cd tpe2-g2-server-1.0-SNAPSHOT
chmod u+x $(ls | egrep run-)
./run-server.sh
```

The condensed command is:
```
cd server/target/ && tar -xzf tpe2-g2-server-1.0-SNAPSHOT-bin.tar.gz && cd tpe2-g2-server-1.0-SNAPSHOT && chmod u+x $(ls | egrep run-) && ./run-server.sh
```

### Queries
To run the client queries, from terminal B run:
```
cd client/target/
tar -xzf tpe2-g2-client-1.0-SNAPSHOT-bin.tar.gz
cd tpe2-g2-client-1.0-SNAPSHOT
chmod u+x $(ls | egrep query)
```

The condensed command is:
```
cd client/target/ && tar -xzf tpe2-g2-client-1.0-SNAPSHOT-bin.tar.gz && cd tpe2-g2-client-1.0-SNAPSHOT && chmod u+x $(ls | egrep query)
```

#### Query 1
To run the query 1, the following command is used:
```
```

#### Query 2
To run the query 2, the following command is used:
```
./query2.sh -Dcity=BUE -Daddresses='192.168.1.192:5701' -DinPath=./../../../examples/ -DoutPath=./../../../examples/result-bue-2.csv
./query2.sh -Dcity=VAN -Daddresses='192.168.1.192:5701' -DinPath=./../../../examples/ -DoutPath=./../../../examples/result-van-2.csv
```

#### Query 3
To run the query 3, the following command is used:
```
./query3.sh -Dcity=BUE -Daddresses='192.168.1.192:5701' -DinPath=./../../../examples/ -DoutPath=./../../../examples/result-bue-3.csv -Dn=3
./query3.sh -Dcity=VAN -Daddresses='192.168.1.192:5701' -DinPath=./../../../examples/ -DoutPath=./../../../examples/result-van-3.csv -Dn=3
```

#### Query 4
To run the query 4, the following command is used:
```
```

#### Query 5
To run the query 5, the following command is used:
```
./query5.sh -Dcity=BUE -Daddresses='192.168.1.192:5701' -DinPath=./../../../examples/ -DoutPath=./../../../examples/result-bue-5.csv
./query5.sh -Dcity=VAN -Daddresses='192.168.1.192:5701' -DinPath=./../../../examples/ -DoutPath=./../../../examples/result-van-5.csv
```