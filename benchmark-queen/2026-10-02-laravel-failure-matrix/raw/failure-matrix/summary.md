| Scenario | Profile | Result | Failed checks |
| --- | --- | --- | --- |
| exception-retry | horizon | pass |  |
| exception-retry | queen | pass |  |
| exception-retry | queen-fast | pass |  |
| exception-once | horizon | pass |  |
| exception-once | queen | pass |  |
| exception-once | queen-fast | pass |  |
| release-and-fail | horizon | pass |  |
| release-and-fail | queen | pass |  |
| release-and-fail | queen-fast | pass |  |
| job-timeout | horizon | pass |  |
| job-timeout | queen | pass |  |
| job-timeout | queen-fast | FAIL | two attempts, both cut short |
| memory-limit | horizon | pass |  |
| memory-limit | queen | pass |  |
| memory-limit | queen-fast | FAIL | every job completed; no failed-job row |
| memory-fatal | horizon | pass |  |
| memory-fatal | queen | pass |  |
| memory-fatal | queen-fast | FAIL | two attempts, both died |
| worker-kill | horizon | pass |  |
| worker-kill | queen | pass |  |
| worker-kill | queen-fast | pass |  |
| master-kill | horizon | pass |  |
| master-kill | queen | pass |  |
| master-kill | queen-fast | pass |  |
| stop-short | horizon | pass |  |
| stop-short | queen | pass |  |
| stop-short | queen-fast | pass |  |
| stop-long | horizon | pass |  |
| stop-long | queen | pass |  |
| stop-long | queen-fast | FAIL | no job completed twice |
| backend-restart | horizon | FAIL | no job completed twice |
| backend-restart | queen | pass |  |
| backend-restart | queen-fast | pass |  |
| pause-short | horizon | pass |  |
| pause-short | queen | pass |  |
| pause-short | queen-fast | pass |  |
| pause-long | horizon | pass |  |
| pause-long | queen | pass |  |
| pause-long | queen-fast | pass |  |
| dispatch-backend-down | horizon | pass |  |
| dispatch-backend-down | queen | pass |  |
| dispatch-backend-down | queen-fast | pass |  |
