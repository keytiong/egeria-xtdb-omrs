# Open Metadata Conformance Test Suite

This is a quick guide on how to setup and run Egeria Open Metadata
Conformance Suite

## Build Crux OMRS
```shell
lein clean
lein install
mvn -f assembly/pom.xml clean install
```

## Install Egeria Open Metadata 
Install Egeria OMAG platform.
```shell
export EGERIA_HOME=/opt/egeria/egeria-omag
```

## Start Egeria OMAG platform
```shell
java \
 -Dserver.port=9443 \
 -Dloader.path=assembly/target \
 -Dserver.ssl.trustStore=$EGERIA_HOME/truststore.p12 \
 -jar \
 $EGERIA_HOME/server/server-chassis-spring-2.8-SNAPSHOT.jar
```

## Start Clojure REPL
```shell
lein with-profile dev repl
```

## Configures Run Conformance Test Suite 
```clojure
;; change to Egeria conformance test suite namespace
(cts)

;; in egeria-conformance-test namespace
;; run (go) to configure SUT and CTS server on the running Egeria OMAG platform
;; as well as starting Kafka Docker container
(go)

;; Start system-under-test server
(start-sut-server)

;; Start conformance-test-suite server
(start-cts-server)
```