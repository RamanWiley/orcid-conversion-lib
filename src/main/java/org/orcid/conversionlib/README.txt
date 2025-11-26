# orcid-conversion-lib (parallel tarball fork)

This zip contains a minimal set of sources to build a parallelised version of the
ORCID conversion CLI.

## Layout

- pom.xml
- src/main/java/org/orcid/conversionlib/App.java
- src/main/java/org/orcid/conversionlib/CommandLineOptions.java
- src/main/java/org/orcid/conversionlib/SchemaVersion.java
- src/main/java/org/orcid/conversionlib/OrcidTranslator.java
- src/main/java/org/orcid/conversionlib/ParallelOrcidArchiveTranslator.java

## Build

From this directory:

    mvn -DskipTests clean package

The fat executable jar will be at:

    target/orcid-conversion-lib-3.0.7-full.jar

## Run (tarball mode)

    java -Xms4g -Xmx4g \
      -jar target/orcid-conversion-lib-3.0.7-full.jar \
      -t \
      -n 8 \
      -i /path/to/ORCID_YYYY_MM_activities.tar.gz \
      -o /path/to/ORCID_YYYY_MM_activities_json.tar.gz \
      -v v3_0 \
      -f xml
