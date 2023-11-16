YCSB

To build the full distribution, with all database bindings:

    mvn clean package

To build a single database binding:

    mvn -pl site.ycsb:mongodb-binding -am clean package
    OR
    mvn -pl mongodb -am compile 

To run a workload:

    ./bin/ycsb run couchdb -P workloads/workloada -p exportfile=results/file.txt 


