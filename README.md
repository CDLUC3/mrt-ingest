# Merritt Ingest Service

This microservice is part of the [Merritt Preservation System](https://github.com/CDLUC3/mrt-doc).

## Purpose

This microservice processes content to be ingested into the Merritt Preservation Repository.

An ingest package can consist of a single file, a container file (zip, tar), or a 
Merritt [manifest](https://github.com/CDLUC3/mrt-doc/wiki/Manifests) file.

Once an ingest package has been prepared, it is sent to the [Merritt Storage Service](https://github.com/CDLUC3/mrt-store).

## Component Diagram
![Flowchart](https://github.com/CDLUC3/mrt-doc/raw/main/diagrams/ingest.mmd.svg)

## Dependencies

This code depends on the following Merritt Libraries.
- [Merritt Core Library](https://github.com/CDLUC3/mrt-core2)
- [CDL Zookeeper Library](https://github.com/CDLUC3/cdl-zk-queue)

## For external audiences
This code is not intended to be run apart from the Merritt Preservation System.

See [Merritt Docker](https://github.com/CDLUC3/merritt-docker) for a description of how to build a test instnce of Merritt.

## Build instructions
This code is deployed as a war file. The war file is built on a Jenkins server.

## Test instructions

## Internal Links

### Deployment and Operations at CDL

https://github.com/CDLUC3/mrt-doc-private/blob/main/uc3-mrt-ingest.md

## Legacy README

### Requirements
```
JDK 1.6(+)
Java servlet container that accepts .war (e.g. tomcat, resin, jetta, ...)
Maven 2
Zookeeper 3.3.1 running and referenced in "queue.txt" (see Running under Tomcat)
zookeeper-recipes-3.3.1.jar Queueing library located at repository 'cdl-zk-queue'
```

### Packaging
```
Start by modifying property files at:
    ingest-conf/properties/{stage,development,local}

    Defining the location of "ingest home" is critical, for example
         ingestServicePath=/dpr/ingest_home
         fileLogger.path=/dpr/ingest_home/logs

Create the distribution package: 
    mvn -Denvironment={stage,development,local} package
```

### Architecture overview
Distribution package contains a war file which defines two servlets, poster and ingest.  

poster handles "batch" submissions, that is multiple objects per request.  Batches are defined in manifest (see Testing section)
Manifests are parsed and then queued in queueing service (Zookeeper).  A daemon process then polls queue and submits a "jobs" to the ingest service, which is housed in the ingest servlet.

If there is no need to process batches, then ingest service can be called directly.  Both ingest and poster support a REST and command line interface.


### Running under Tomcat
Unpack distribution and move ingest_home/ template directory to location defined in properties (see above).
Modify template directory to customize user environment.

```
Ingest home structure:
    ingest-info.txt	(ingest configuration)
    logs/		(logging)
    profiles/		(user profiles)
    queue/		(ingest service working directory)
    queue.txt		(defines Zookeeper queue configuration)
    stores.txt		(defines Storage service)

Deploy war file in Tomcat noting that the hostname and port of servlet container must be defined at:
    ingest_home/ingest-info.txt (access-uri)

To allow ingest service to access data located in ingest home define a Context element in server.xml, for example
    <Context path="/ingestqueue" allowLinking="true" docBase="webapps/ingestqueue"/>

    Then create a symbolic link in the webapps directory, linking to ingest home queue directory.
	ln -s /dpr/ingest_home/queue ingestqueue
```


### Running under Winstone
While this is a simpler deployment approach, minimal testing has been done with the Winstone servlet container (http://winstone.sourceforge.net)

```
java -jar winstone-0.9.10.jar --warfile=mrt-ingestwar-1.0-SNAPSHOT.war --httpPort=8090
```

### Test
```
See online help pages for manifest information and sample data: http://merritt.cdlib.org/help

Example manifest submission using queue (synchronous):
    curl --silent  \
        -F "file=@example.checkm" \
        -F "type=container-batch-manifest" \
        -F "submitter=sample-user" \
        -F "responseForm=xml" \
        -F "profile=sample-profile" \
        http://example.org:8080/poster/submit
Example manifest submission directly to Ingest (synchronous):
    curl --silent  \
        -F "file=@example.checkm" \
        -F "type=container-batch-manifest" \
        -F "submitter=sample-user" \
        -F "responseForm=xml" \
        -F "profile=sample-profile" \
        http://example.org:8080/ingest/submit-object
```
### Logs
Check Tomcat logs.

