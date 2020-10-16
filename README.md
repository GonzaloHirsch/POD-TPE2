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