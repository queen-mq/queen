package main

// verify.go — per-tenant DELIVERY CORRECTNESS accounting for -mode cloud.
//
// This is the output the whole campaign hinges on: a throughput number with
// unverified delivery is worthless. Every message carries (tenant index,
// monotonic sequence); the producer records the sequences it actually got a
// 2xx for, the consumer records the sequences it actually received, and at the
// end of the run (after the drain phase) we diff the two bitmaps:
//
//	missing    = sent AND NOT received   -> LOSS
//	duplicate  = a sequence delivered more than once (counted live)
//	extra      = received AND NOT sent   -> a message the producer never
//	             recorded as sent: either a push that timed out client-side but
//	             landed server-side, or a fabricated delivery. Reported
//	             separately from loss because the cause is different.
//	cross      = a message whose payload tenant != the consumer's tenant ->
//	             TENANT ISOLATION BREACH. In the shared-queue shape (every
//	             tenant using the same queue name and group name) this is the
//	             single most important check in the campaign.
//
// Two bits per message per tenant: 1M messages costs 250 KB, so the verifier
// is affordable at ceiling rates and is therefore ON by default (HARD RULE 5:
// correctness is measured IN the perf run, not in a separate pass).

import (
	"math/bits"
	"sync"
)

type tenantVerify struct {
	mu   sync.Mutex
	sent []uint64 // bit i set => seq i+1 was pushed and acknowledged 2xx
	rcv  []uint64 // bit i set => seq i+1 was delivered at least once

	SeqAssigned int64 // highest sequence handed out by the pacer
	SentOK      int64 // pushes the server accepted
	SentFail    int64 // pushes that errored (excluded from the expected set)
	SentLostFI  int64 // fault-injected: recorded as sent, deliberately not pushed
	Received    int64 // delivery events for THIS tenant's own messages
	Dup         int64 // delivery events for a sequence already seen
	CrossIn     int64 // messages received by this tenant that belong to another
	Undecodable int64 // delivered message whose payload had no (t,s)
}

func newTenantVerify() *tenantVerify { return &tenantVerify{} }

func setBit(bmp *[]uint64, seq int64) (already bool) {
	if seq <= 0 {
		return false
	}
	idx := (seq - 1) / 64
	off := uint((seq - 1) % 64)
	for int64(len(*bmp)) <= idx {
		grow := make([]uint64, (idx+1)*2)
		copy(grow, *bmp)
		*bmp = grow
	}
	w := (*bmp)[idx]
	if w&(1<<off) != 0 {
		return true
	}
	(*bmp)[idx] = w | (1 << off)
	return false
}

// markSent records a contiguous block of sequences as successfully pushed.
func (v *tenantVerify) markSent(first, n int64) {
	v.mu.Lock()
	for s := first; s < first+n; s++ {
		setBit(&v.sent, s)
	}
	v.SentOK += n
	v.mu.Unlock()
}

// markSentFI records sequences the fault injector deliberately DID NOT push
// while still claiming them as sent — the checker must then report them as
// missing. This is how we prove the checker can fail.
func (v *tenantVerify) markSentFI(first, n int64) {
	v.mu.Lock()
	for s := first; s < first+n; s++ {
		setBit(&v.sent, s)
	}
	v.SentOK += n
	v.SentLostFI += n
	v.mu.Unlock()
}

func (v *tenantVerify) markSentFail(n int64) {
	v.mu.Lock()
	v.SentFail += n
	v.mu.Unlock()
}

// recordRecv folds one delivered sequence in. Duplicates are counted here (the
// bitmap only remembers "seen"), everything else is diffed at the end.
func (v *tenantVerify) recordRecv(seq int64) {
	v.mu.Lock()
	if setBit(&v.rcv, seq) {
		v.Dup++
	}
	v.Received++
	v.mu.Unlock()
}

func (v *tenantVerify) recordCross() {
	v.mu.Lock()
	v.CrossIn++
	v.mu.Unlock()
}

func (v *tenantVerify) recordUndecodable() {
	v.mu.Lock()
	v.Undecodable++
	v.mu.Unlock()
}

func (v *tenantVerify) noteAssigned(last int64) {
	v.mu.Lock()
	if last > v.SeqAssigned {
		v.SeqAssigned = last
	}
	v.mu.Unlock()
}

// TenantResult is the per-tenant verdict, marshalled into the run JSON.
type TenantResult struct {
	Idx         int     `json:"idx"`
	Tenant      string  `json:"tenant"`
	Cluster     string  `json:"cluster"`
	Queue       string  `json:"queue"`
	SeqAssigned int64   `json:"seqAssigned"`
	SentOK      int64   `json:"sentOk"`
	SentFail    int64   `json:"sentFail"`
	SentLostFI  int64   `json:"sentLostFaultInjected"`
	Received    int64   `json:"received"`
	Missing     int64   `json:"missing"`
	Duplicate   int64   `json:"duplicate"`
	Extra       int64   `json:"extra"`
	CrossIn     int64   `json:"crossTenantIn"`
	Undecodable int64   `json:"undecodable"`
	FirstMissed []int64 `json:"firstMissingSeqs,omitempty"`
	OK          bool    `json:"ok"`
}

// result diffs the two bitmaps and produces the verdict for this tenant.
func (v *tenantVerify) result(idx int, tenant, cluster, queue string) TenantResult {
	v.mu.Lock()
	defer v.mu.Unlock()

	n := len(v.sent)
	if len(v.rcv) > n {
		n = len(v.rcv)
	}
	var missing, extra int64
	firstMissed := make([]int64, 0, 8)
	for i := 0; i < n; i++ {
		var s, r uint64
		if i < len(v.sent) {
			s = v.sent[i]
		}
		if i < len(v.rcv) {
			r = v.rcv[i]
		}
		m := s &^ r
		e := r &^ s
		missing += int64(bits.OnesCount64(m))
		extra += int64(bits.OnesCount64(e))
		for m != 0 && len(firstMissed) < 8 {
			b := bits.TrailingZeros64(m)
			firstMissed = append(firstMissed, int64(i)*64+int64(b)+1)
			m &^= 1 << uint(b)
		}
	}
	return TenantResult{
		Idx:         idx,
		Tenant:      tenant,
		Cluster:     cluster,
		Queue:       queue,
		SeqAssigned: v.SeqAssigned,
		SentOK:      v.SentOK,
		SentFail:    v.SentFail,
		SentLostFI:  v.SentLostFI,
		Received:    v.Received,
		Missing:     missing,
		Duplicate:   v.Dup,
		Extra:       extra,
		CrossIn:     v.CrossIn,
		Undecodable: v.Undecodable,
		FirstMissed: firstMissed,
		OK:          missing == 0 && v.Dup == 0 && extra == 0 && v.CrossIn == 0 && v.Undecodable == 0,
	}
}
