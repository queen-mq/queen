-- Is "missing" real loss, or messages still sitting un-popped in the queues?
--
-- goload calls it LOSS when consumers were idle at the cutoff. That heuristic
-- assumes an idle consumer has seen everything -- true at low cardinality. Here
-- a consumer samples pop-partitions=10 of 5,000 partitions per queue, so it can
-- report empty while messages wait in the 4,990 it did not visit. Undelivered
-- backlog is therefore the number that decides it: last_offset is the highest
-- written offset per partition, committed the last acked one per group.
SELECT c.consumer_group AS grp,
       count(*) FILTER (WHERE p.last_offset > c.committed) AS parts_behind,
       COALESCE(sum(p.last_offset - c.committed)
                FILTER (WHERE p.last_offset > c.committed), 0) AS undelivered
FROM queen.log_partitions p
JOIN queen.log_consumers c ON c.partition_id = p.id
GROUP BY c.consumer_group
ORDER BY undelivered DESC;
