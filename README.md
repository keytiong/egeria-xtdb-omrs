# Egeria Crux Open Metadata Repository

[Egeria](https://www.github.com/odpi/egeria) open metadata repository implementation in Juxt [Crux](https://www.github.com/juxt/crux).

This project is still in early development phase. It is not (yet) conform to Egeria repository conformance test suite.

See [Egeria Crux Plugin Repository Connector](https://github.com/odpi/egeria-connector-crux) for a more advanced implementation.

## Build
```shell
lein clean
lein install
```

## Package
```shell
mvn -f assembly/pom.xml clean package

# Resultant uberjar is located at
# assembly/target/egeria-crux-orms-assembly-X.X.X-SNAPSHOT-jar-with-dependencies.jar
```