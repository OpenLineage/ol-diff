
# Ol-diff

**Use-case**: Imagine a scenario where one has to upgrade OpenLineage connector with custom changes implemented and 
there is selected set of jobs that can be run multiple times to produce OpenLineage events. In this case, one is able
to run each job with the previous and the new version of the connector and gather the events: either through the `FileTransport`
or logs written by the `ConsoleTransport`.

Then `ol-diff` can be used to compare the events content. 
`ol-diff` compares lineage events cumulatively. For each facet from the events, it
triggers a separate test to verify if facets exists in the next version 
and if the fields from the previous version are present in the next version.

Simple example usage:
```
./ol-diff.sh
```
This will start Gradle docker container and run the tests.

## Verification

*  **Job verification**
   * Verifies job name and namespace
   * Verifies all the facets in previous version are present in the next version.
   * For each facet in previous version, it verifies if the fields from previous version are present in the next version. New facets or fields are accepted.
*  **Run verification**
   * Verifies all the facets in previous version are present in the next version.
   * For each facet in previous version, it verifies if the fields from previous version are present in the next version. New facets or fields are accepted.
*  **Dataset verification**
   * Verifies both connectors detect exact set of datasets.
   * For each dataset, it verifies input facets, output facets and facets. For each facet in previous version, it verifies if the fields from previous version are present in the next version. New facets or fields are accepted.
*  **Application events verification** 
   * TODO 

### Datasets verification

## Future work

TODO: 
 * Check application events - and their facets,

## Development

As a developer you can run tests with an extra param to include internal tests - tests that test the tool itself. 

```
docker run --rm -u gradle -v "$PWD":/home/gradle/project -w /home/gradle/project gradle:jdk17-ubi  gradle clean test -Pprev.path=examples/success/prev.txt -Pnext.path=examples/success/next.txt -Pinternal.tests=true
open build/reports/tests/test/index.html
```

Tests should then include extra classes like `JobHelpersTest` or `RunHelperTest`.