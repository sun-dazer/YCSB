# YCSB - Benchmarking of MongoDB and CouchDB

## Prerequisites
Ensure you have the following installed:
- Java Development Kit (JDK)
- Apache Maven

## Building the Project

### Building the Full Distribution
To compile and package the entire project with all database bindings, use the following Maven command:

    mvn clean package

### Building a Single Database Binding
If you prefer to build a single database binding, like MongoDB, use either of the following commands:

    mvn -pl site.ycsb:mongodb-binding -am clean package
    OR
    mvn -pl mongodb -am compile 

## Running a Workload

To execute a workload on a specific database, such as MongoDB, use the following command:

    ./bin/ycsb run mongodb -P workloads/workloada -p exportfile=results/file.txt

This command runs a workload defined in `workloada` and exports the results to `results/file.txt`.
