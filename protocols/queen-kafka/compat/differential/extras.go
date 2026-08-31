package main

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	scenarios = append(scenarios, scenario{
		name: "extras",
		desc: "what a broker does with a version it does not know, an API key it does not know, and an API it does not implement",
		run:  scenExtras,
	})
}

func frameFor(apiKey, version int16, corr int32, clientID string, flexHeader bool, body []byte) []byte {
	var f []byte
	f = append(f, 0, 0, 0, 0)
	f = binary.BigEndian.AppendUint16(f, uint16(apiKey))
	f = binary.BigEndian.AppendUint16(f, uint16(version))
	f = binary.BigEndian.AppendUint32(f, uint32(corr))
	f = binary.BigEndian.AppendUint16(f, uint16(len(clientID)))
	f = append(f, clientID...)
	if flexHeader {
		f = append(f, 0)
	}
	f = append(f, body...)
	binary.BigEndian.PutUint32(f[:4], uint32(len(f)-4))
	return f
}

func readErrKind(err error) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "no response, connection left open"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "broken pipe") {
		return "connection closed without a response"
	}
	return "read failed: " + err.Error()
}

func probeFrame(c *runctx, name string, frame []byte, wait time.Duration) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad(name+".dial", err)
		return
	}
	defer k.Close()
	if err := k.c.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		c.rec.bad(name+".deadline", err)
		return
	}
	if _, err := k.c.Write(frame); err != nil {
		c.rec.add(name+".answer", "the write itself failed: %v", err)
		return
	}
	body, err := k.recvRaw(false, wait)
	if err != nil {
		c.rec.add(name+".answer", "%s", readErrKind(err))
		return
	}
	c.rec.add(name+".answer", "a response body of %d bytes", len(body))
	if len(body) >= 2 {
		c.rec.add(name+".first_int16", "%s", errName(int16(binary.BigEndian.Uint16(body[:2]))))
	}
	c.rec.info(name+".head_hex", "%s", hex.EncodeToString(body[:min(24, len(body))]))
}

func scenExtras(c *runctx) {
	// A version of a normal API that no broker supports. Metadata is
	// flexible from v9, so a header parser that follows the spec reads
	// version 99's header as flexible — which is how the request is written
	// here.
	probeFrame(c, "metadata_v99",
		frameFor(3, 99, 1, "qk-diff", true, []byte{0x00, 0x00, 0x00, 0x00, 0x00}), 8*time.Second)

	// An API key nobody has.
	probeFrame(c, "unknown_api_key_199",
		frameFor(199, 0, 1, "qk-diff", false, nil), 8*time.Second)

	// Produce at v2, below the floor the facade advertises (3-9) and inside
	// what Kafka 3.9 still accepts.
	probeFrame(c, "produce_v2",
		frameFor(0, 2, 1, "qk-diff", false,
			[]byte{0x00, 0x01, 0x00, 0x00, 0x27, 0x10, 0x00, 0x00, 0x00, 0x00}), 8*time.Second)

	// An API that exists and is deliberately not implemented: the plan rules
	// out transactions and EOS, and franz-go's default producer opens with
	// this one.
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("initproducerid.dial", err)
		return
	}
	defer k.Close()
	req := kmsg.NewInitProducerIDRequest()
	req.TransactionTimeoutMillis = 60_000
	resp, _, err := k.doT(&req, 0, 8*time.Second)
	if err != nil {
		c.rec.add("initproducerid.answer", "%s", readErrKind(err))
		return
	}
	ip := resp.(*kmsg.InitProducerIDResponse)
	c.rec.add("initproducerid.answer", "a response")
	c.rec.add("initproducerid.error_code", "%s", errName(ip.ErrorCode))
	c.rec.info("initproducerid.producer_id", "%d", ip.ProducerID)
}
