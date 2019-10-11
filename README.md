# VoltDB H2o MOJO Demo

## Introduction

## Installation

### Copy the H2O library to VoltDB servers

H2O generates code that uses a library called 'h2o-genmodel.jar'. This needs to be copied into the 'lib' directory underneath the VoltDB code directory for each node in the cluster.

To find the correct location you can log into the server and do:

    Davids-MacBook-Pro-7:~ drolfe$ which voltdb
    /Users/drolfe/Desktop/InstallsOfVolt/voltdb-ent-9.1.1/bin/voltdb

The correct directory is thus:

    /Users/drolfe/Desktop/InstallsOfVolt/voltdb-ent-9.1.1/lib
    
So we need to install the right version of h2o-genmodel.jar:

    cp h2o-genmodel.jar /Users/drolfe/Desktop/InstallsOfVolt/voltdb-ent-9.1.1/lib
    
This is a once off process that needs to be done for each node in the cluster. 

If you don't do it you'll get:

    ERROR org.voltdb.h20mojo.client.VoltDBH2OMojoClient - The response from host 0 for 
    @VerifyCatalogAndWriteJar returned failures: unexpected error verifying classes or 
    preparing procedure runners: Error loading class 'mojoprocs.IsFlightLate': 
    java.lang.NoClassDefFoundError for hex/genmodel/GenModel

    