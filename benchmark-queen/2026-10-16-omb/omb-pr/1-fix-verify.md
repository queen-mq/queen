`mvn verify` fails on master since #443. Because of that, the Build and test job of every pull request
fails in `benchmark-framework` before it reaches the pull request's own code. Run 38648660840 is one
example: the latest Dependabot bump, from 2026-10-14, failed on `spotless:check` in
`MessageSizeDistributionTest.java`.

There are two causes, both in files #443 added or changed:

- **`spotless:check`.** `MessageSizeDistribution`, `MessageSizeDistributionTest`, `Workload`,
  `LocalWorker` and `ProducerWorkAssignment` were not formatted. This PR runs `mvn spotless:apply` on
  them, and their diff is formatting only.
- **spotbugs `EI_EXPOSE_REP`.** `MessageSizeDistribution.getBuckets()` returned its internal list. It
  now returns `Collections.unmodifiableList(buckets)`. No caller is affected:
  - nothing in the repository calls it;
  - the class is only ever built through its constructor, never by Jackson.

Tested with the workflow's own command, `mvn --no-transfer-progress --batch-mode verify`, on JDK 17
(Temurin): BUILD SUCCESS.
