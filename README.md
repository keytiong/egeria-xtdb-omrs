# Egeria XTDB Open Metadata Repository

[Egeria](https://www.github.com/odpi/egeria) open metadata repository implementation in Juxt [XTDB](https://www.github.com/juxt/xtdb).

This project is still in early development phase. It is not (yet) conform to Egeria repository conformance test suite.

See [Egeria XTDB Plugin Repository Connector](https://github.com/odpi/egeria-connector-xtdb) for a more advanced implementation.

## Build
```shell
lein clean
lein install
```

## Package
```shell
mvn -f assembly/pom.xml clean package

# Resultant uberjar is located at
# assembly/target/egeria-xtdb-orms-assembly-X.X.X-SNAPSHOT-jar-with-dependencies.jar
```