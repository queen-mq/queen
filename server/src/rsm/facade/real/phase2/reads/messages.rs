//! Historical message pages: seek and rank index metadata before reading payloads.

use std::collections::BTreeMap;

use super::{message_status, newest_stamp, MsgPart, Picked, QLogReader, Record, RsmError};
use crate::frames::unpack_frames_ref;

pub(super) struct Window<'a> {
    pub from_us: i64,
    pub to_us: i64,
    pub now: i64,
    pub status: Option<&'a str>,
    pub offset: usize,
    pub limit: usize,
}

pub(super) fn indexed_page(
    qlog: &QLogReader,
    tenant: &str,
    cands: &[MsgPart],
    window: &Window<'_>,
) -> Result<Vec<Picked>, RsmError> {
    let need = window.offset.saturating_add(window.limit);
    // `record` is the append's base offset until the final page is selected.
    let mut best: Vec<Picked<u64>> = Vec::new();
    for (ci, c) in cands.iter().enumerate() {
        if best.len() >= need
            && best
                .last()
                .is_some_and(|p| newest_stamp(&c.part.row) < p.created_at_us)
        {
            break;
        }
        let qid = QLogReader::queue_id_of(tenant, &c.part.row.queue);
        let low = c.part.row.log_start;
        let mut high = (c.part.row.last_offset + 1).max(0) as u64;
        'partition: while high > low {
            let Some(rec) = qlog.record_before(qid, c.part.pid, high, window.to_us) else {
                break;
            };
            if rec.created_at_us < window.from_us || rec.end <= low {
                break;
            }
            // Clip batches at both snapshot watermarks, including a retained
            // prefix or an applied tail that falls inside a batch.
            for offset in (rec.base_offset.max(low)..rec.end.min(high)).rev() {
                let key = (rec.created_at_us, offset);
                if best.len() >= need
                    && best
                        .last()
                        .is_some_and(|p| key <= (p.created_at_us, p.offset))
                {
                    break 'partition;
                }
                let (status, consumed_by, total_groups) =
                    message_status(offset, &c.cursors, &c.dlq, window.now);
                if window.status.is_some_and(|s| s != status) {
                    continue;
                }
                let at = best.partition_point(|p| (p.created_at_us, p.offset) > key);
                best.insert(
                    at,
                    Picked {
                        created_at_us: rec.created_at_us,
                        offset,
                        cand: ci,
                        status,
                        consumed_by,
                        total_groups,
                        record: rec.base_offset,
                    },
                );
                best.truncate(need);
            }
            high = rec.base_offset;
        }
    }

    // Group by append: a page containing 100 frames from one compressed batch
    // must read/decompress that batch once. Skipped pages and losing candidates
    // never reach the payload reader. Preserve the rank when groups interleave.
    let mut batches: BTreeMap<(usize, u64), Vec<(usize, Picked<u64>)>> = BTreeMap::new();
    for (rank, pick) in best
        .into_iter()
        .skip(window.offset)
        .take(window.limit)
        .enumerate()
    {
        batches
            .entry((pick.cand, pick.record))
            .or_default()
            .push((rank, pick));
    }
    let mut page = Vec::new();
    for ((ci, _), picks) in batches {
        let part = &cands[ci].part;
        let qid = QLogReader::queue_id_of(tenant, &part.row.queue);
        let Some(rec) = qlog
            .read_owned(qid, part.pid, picks[0].1.offset)
            .map_err(|e| RsmError::Internal(format!("qlog read: {e}")))?
        else {
            continue; // retention removed the append after the index read
        };
        let Some(frames) = unpack_frames_ref(&rec.payload) else {
            continue;
        };
        for (rank, pick) in picks {
            let Some(i) = pick
                .offset
                .checked_sub(rec.base_offset)
                .and_then(|i| usize::try_from(i).ok())
            else {
                continue;
            };
            let Some(f) = frames.get(i) else { continue };
            page.push((
                rank,
                Picked {
                    created_at_us: pick.created_at_us,
                    offset: pick.offset,
                    cand: pick.cand,
                    status: pick.status,
                    consumed_by: pick.consumed_by,
                    total_groups: pick.total_groups,
                    record: Record {
                        queue: part.row.queue.clone(),
                        partition: part.row.partition.clone(),
                        partition_id: part.row.uuid,
                        offset: pick.offset,
                        segment_base: rec.base_offset,
                        frame_idx: i,
                        created_at_us: rec.created_at_us,
                        id: f.message_id,
                        txn: f.txn.to_string(),
                        trace_id: f.trace_id,
                        producer_sub: f.producer_sub.map(str::to_string),
                        payload: f.payload.to_vec(),
                        encrypted: f.encrypted,
                    },
                },
            ));
        }
    }
    page.sort_by_key(|(rank, _)| *rank);
    Ok(page.into_iter().map(|(_, pick)| pick).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::{pack_frames, FrameIn};
    use crate::rsm::qlog::{set::QLogSet, QLogOptions};
    use crate::rsm::store::rows::PartitionRow;
    use std::path::PathBuf;

    struct Fixture {
        root: PathBuf,
        logs: QLogSet,
        parts: Vec<MsgPart>,
        seq: u64,
    }

    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "queen-message-page-{}",
                crate::frames::uuid_bytes_to_string(&crate::util::uuidv7_bytes())
            ));
            Self {
                logs: QLogSet::new(root.clone(), QLogOptions::testing(1024)),
                root,
                parts: Vec::new(),
                seq: 0,
            }
        }

        fn append(&mut self, pid: u64, stamp: i64, count: u32) {
            let ci = self
                .parts
                .iter()
                .position(|c| c.part.pid == pid)
                .unwrap_or_else(|| {
                    self.parts.push(MsgPart {
                        part: super::super::Part {
                            pid,
                            row: PartitionRow::new(
                                [pid as u8; 16],
                                "tenant",
                                "orders",
                                &pid.to_string(),
                                0,
                            ),
                            sealed: Vec::new(),
                        },
                        cursors: Vec::new(),
                        dlq: Default::default(),
                        namespace: None,
                        task: None,
                        priority: 0,
                    });
                    self.parts.len() - 1
                });
            let part = &mut self.parts[ci].part;
            let base = (part.row.last_offset + 1) as u64;
            let names: Vec<_> = (0..count)
                .map(|i| format!("{pid}-{}", base + i as u64))
                .collect();
            let frames: Vec<_> = names
                .iter()
                .map(|txn| FrameIn {
                    message_id: [pid as u8; 16],
                    txn,
                    trace_id: Some([7; 16]),
                    producer_sub: Some("producer"),
                    payload: b"{\"value\":42}",
                    encrypted: false,
                })
                .collect();
            self.seq += 1;
            self.logs.buffer(
                "tenant",
                "orders",
                self.seq,
                pid,
                base,
                count,
                stamp,
                &vec![0; count as usize * 16],
                &pack_frames(&frames),
            );
            self.logs.flush().unwrap();
            part.row.last_offset += count as i64;
            part.row.last_created_at_us = stamp;
            part.row.last_write_at_us = stamp;
            self.parts
                .sort_by_key(|p| std::cmp::Reverse(newest_stamp(&p.part.row)));
        }

        fn page(
            &self,
            from: i64,
            to: i64,
            offset: usize,
            limit: usize,
            status: Option<&str>,
        ) -> Vec<Picked> {
            indexed_page(
                &self.logs.reader(),
                "tenant",
                &self.parts,
                &Window {
                    from_us: from,
                    to_us: to,
                    now: 1000,
                    offset,
                    limit,
                    status,
                },
            )
            .unwrap()
        }

        fn damage_payload(&self, pid: u64, offset: u64) {
            use std::os::unix::fs::FileExt;
            let qid = QLogReader::queue_id_of("tenant", "orders");
            let lid = self.logs.log_id_for(qid, pid);
            let log = self.logs.log(lid).unwrap();
            let loc = log.read().unwrap().locate(pid, offset).unwrap();
            let path = self.root.join(format!("q{lid}/r{:08}.qlog", loc.file_id));
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            let at = loc.offset + loc.len as u64 - 1;
            let mut b = [0];
            file.read_exact_at(&mut b, at).unwrap();
            b[0] ^= 0xff;
            file.write_all_at(&b, at).unwrap();
            assert!(
                self.logs.reader().read_owned(qid, pid, offset).is_err(),
                "fixture must fail a payload read"
            );
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn positions(page: &[Picked]) -> Vec<(i64, u64, String)> {
        page.iter()
            .map(|p| (p.created_at_us, p.offset, p.record.partition.clone()))
            .collect()
    }

    #[test]
    fn indexed_page_preserves_global_order_paging_status_and_watermarks() {
        let mut f = Fixture::new();
        f.append(1, 100, 4);
        f.append(2, 150, 2);
        f.append(1, 200, 3);
        f.append(2, 210, 4);
        f.append(1, 300, 3);
        let p = f.parts.iter_mut().find(|p| p.part.pid == 1).unwrap();
        p.part.row.log_start = 1;
        p.part.row.last_offset = 8; // snapshot ends inside the last batch
        p.dlq.extend([3, 5]);
        let mut expected = Vec::new();
        for (pid, stamp, lo, hi) in [
            (1, 100, 1, 4),
            (2, 150, 0, 2),
            (1, 200, 4, 7),
            (2, 210, 2, 6),
            (1, 300, 7, 9),
        ] {
            for off in lo..hi {
                expected.push((stamp, off, pid.to_string()));
            }
        }
        expected.sort_by(|a, b| (b.0, b.1).cmp(&(a.0, a.1)));
        for from in [0, 150, 200, 301] {
            for to in [100, 200, 201, 301] {
                for offset in [0, 1, 5, 100] {
                    for limit in [1, 3, 100] {
                        for status in [None, Some("pending"), Some("dead_letter")] {
                            let want: Vec<_> = expected
                                .iter()
                                .filter(|(t, off, pid)| {
                                    let dead = pid == "1" && [3, 5].contains(off);
                                    *t >= from
                                        && *t < to
                                        && status.is_none_or(|s| (s == "dead_letter") == dead)
                                })
                                .skip(offset)
                                .take(limit)
                                .cloned()
                                .collect();
                            let page = f.page(from, to, offset, limit, status);
                            assert_eq!(positions(&page), want,
                                "from={from}, to={to}, offset={offset}, limit={limit}, status={status:?}");
                            for p in page {
                                assert_eq!(
                                    p.record.txn,
                                    format!("{}-{}", p.record.partition, p.offset)
                                );
                                assert_eq!(p.record.payload, b"{\"value\":42}");
                                assert_eq!(p.record.trace_id, Some([7; 16]));
                                assert_eq!(p.record.producer_sub.as_deref(), Some("producer"));
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn indexed_page_filters_cursor_status_before_paging() {
        use crate::rsm::store::rows::cursor_fresh;
        let mut f = Fixture::new();
        f.append(1, 100, 6);
        let mut a = cursor_fresh(4, 0);
        a.lease_expires_at_us = Some(1001);
        let b = cursor_fresh(1, 0);
        f.parts[0].cursors = vec![("a".into(), a), ("b".into(), b)];
        f.parts[0].dlq.insert(3);
        let completed = f.page(0, 200, 0, 100, Some("completed"));
        assert_eq!(
            positions(&completed),
            [(100, 1, "1".into()), (100, 0, "1".into())]
        );
        assert!(completed
            .iter()
            .all(|p| p.consumed_by == 2 && p.total_groups == 2));
        let processing = f.page(0, 200, 1, 1, Some("processing"));
        assert_eq!(positions(&processing), [(100, 4, "1".into())]);
        assert_eq!(
            (processing[0].consumed_by, processing[0].total_groups),
            (1, 2)
        );
        assert!(f.page(0, 200, 0, 100, Some("pending")).is_empty());
    }

    #[test]
    fn historical_pages_do_not_read_newer_losing_or_skipped_payloads() {
        let mut f = Fixture::new();
        f.append(1, 100, 3);
        f.append(1, 200, 3);
        f.append(1, 300, 3000);
        f.append(2, 150, 3);
        // These checks would fail if the list touched any payload outside its
        // final page, even once. They are deterministic regression guards,
        // independent of disk speed or the machine running the tests.
        f.damage_payload(1, 6); // the whole suffix newer than `to`
        f.damage_payload(2, 0); // a losing partition
        assert_eq!(
            positions(&f.page(0, 300, 0, 2, None)),
            [(200, 5, "1".into()), (200, 4, "1".into())]
        );
        f.damage_payload(1, 3); // page skipped with offset=6 below
        assert_eq!(
            positions(&f.page(0, 300, 6, 2, None)),
            [(100, 2, "1".into()), (100, 1, "1".into())]
        );
        f.parts
            .iter_mut()
            .find(|p| p.part.pid == 1)
            .unwrap()
            .dlq
            .insert(1);
        assert_eq!(
            positions(&f.page(0, 300, 0, 1, Some("dead_letter"))),
            [(100, 1, "1".into())]
        );
    }
}
