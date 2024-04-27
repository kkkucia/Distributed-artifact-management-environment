# Distributed artifact management environment

A simple artifact storage system based on Ray cluster.

## System Description

Implementation of a distributed system for storing artifacts based on a distributed file system. The system follows a scheme similar to Hadoop Distributed File System (HDFS).

The artifacts consist of a name and a long string, which is divided among multiple nodes for storage. The system comprises one name node and several storage nodes, all of which are actors. The name node actor distributes artifacts to storage nodes in chunks and monitors them. Multiple copies of these blocks exist in the system to enhance data availability and transfer, similar to HDFS. Storage nodes experience failures, indicated by a change in status. The name node tracks all fragments and manages copies to ensure data consistency across the system.

## Client shell description

User can perform the following operations from the interactive shell:

- upload - by providing an artifact name and content
- update the artifact content already present in the storage
- user can delete artifacts present in the storage
- user can get the artifact
- user can list all the artifacts
- user can list the status of the cluster
- user can list all storage nodes
- user can list all info about one storage node
- user can exit the shell

## Requirements

1. [Python 3.9](https://www.python.org/downloads/release/python-390/)
2. [Docker (version 25.0.3 or higher)](https://www.docker.com/)
3. [Ray (use "ray[default]" installation)](https://docs.ray.io/en/latest/ray-overview/installation.html)
4. [More info about Ray](https://www.ray.io/)

### Run Ray Server

Go into RayServer directory:

```
cd RayServer
```

### Build Container

Run the following command in this folder to build the ray container:

```
docker build -t rayserver .
```

### Configure Ray Cluster

A docker compose file was created to configure the cluster. Build the cluster with:

```
docker-compose up
```

Or optionally run in detatched mode by adding -d.

The Ray dashboard should now be accessible here: http://localhost:8265

Once you are done, tear down the cluster with

```
docker-compose down
```

### Ports

The following ports are exposed:

- `8265` Ray dashboard
- `6379` Reddis port to allow external workers to join
- `10001` Head node access port to connect external Ray client

### Run Interactive Shell

To run client interactive shell:
(Important: check your python version and check your ray installation)

```
python main.py
```
