package egress

// A tiny Core NATS wire-protocol broker for the SDK conformance test. The SDK
// cannot import internal/testutil (see docs/implementation-history.md#p2-24), so this is a
// trimmed, package-local port of internal/testutil.FakeNATSServer: enough of
// the NATS text protocol for the real nats.go client to register, heartbeat,
// and run the full assignment/stream protocol against a genuine *nats.Conn
// (required so Worker's msg.Respond calls work).

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
)

const (
	conformanceWireFieldsSUB  = 3
	conformanceWireFieldsPUB  = 3
	conformanceWireMaxPayload = 2
)

type conformanceWireServer struct {
	ln         net.Listener
	maxPayload int

	mu        sync.Mutex
	clients   map[*conformanceWireClient]struct{}
	bySubject map[string]map[*conformanceWireSub]struct{}
}

type conformanceWireClient struct {
	server *conformanceWireServer
	conn   net.Conn

	wmu  sync.Mutex
	mu   sync.Mutex
	subs map[string]*conformanceWireSub
}

type conformanceWireSub struct {
	client  *conformanceWireClient
	subject string
	sid     string
}

func newConformanceWireServer(t testing.TB, maxPayload int) *conformanceWireServer {
	t.Helper()

	lc := net.ListenConfig{}

	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake NATS: %v", err)
	}

	srv := &conformanceWireServer{
		ln:         ln,
		maxPayload: maxPayload,
		clients:    make(map[*conformanceWireClient]struct{}),
		bySubject:  make(map[string]map[*conformanceWireSub]struct{}),
	}
	go srv.acceptLoop()

	t.Cleanup(srv.Close)

	return srv
}

func (s *conformanceWireServer) URL() string {
	return "nats://" + s.ln.Addr().String()
}

func (s *conformanceWireServer) Close() {
	_ = s.ln.Close()

	s.mu.Lock()

	clients := make([]*conformanceWireClient, 0, len(s.clients))
	for c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()

	for _, c := range clients {
		_ = c.conn.Close()
	}
}

func (s *conformanceWireServer) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}

		client := &conformanceWireClient{server: s, conn: conn, subs: make(map[string]*conformanceWireSub)}
		s.mu.Lock()
		s.clients[client] = struct{}{}
		s.mu.Unlock()

		go client.serve()
	}
}

func (c *conformanceWireClient) serve() {
	defer c.close()

	err := c.writeInfo()
	if err != nil {
		return
	}

	reader := bufio.NewReader(c.conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		err = c.handleLine(line, reader)
		if err != nil {
			return
		}
	}
}

func (c *conformanceWireClient) handleLine(line string, reader *bufio.Reader) error {
	switch {
	case strings.HasPrefix(line, "PING"):
		return c.writeString("PONG\r\n")
	case strings.HasPrefix(line, "PONG"):
		return nil
	case strings.HasPrefix(line, "CONNECT"):
		return nil
	case strings.HasPrefix(line, "SUB "):
		return c.handleSub(line)
	case strings.HasPrefix(line, "UNSUB "):
		return c.handleUnsub(line)
	case strings.HasPrefix(line, "PUB "):
		return c.handlePub(line, reader)
	default:
		return nil
	}
}

func (c *conformanceWireClient) writeInfo() error {
	info := fmt.Sprintf(`INFO {"server_id":"straw-sdk-conformance","version":"1.0.0","proto":1,"max_payload":%d}`+"\r\n", c.server.maxPayload)

	return c.writeString(info)
}

var errConformanceWireInvalidSUB = errors.New("invalid SUB line")

func (c *conformanceWireClient) handleSub(line string) error {
	fields := strings.Fields(line)
	if len(fields) != conformanceWireFieldsSUB && len(fields) != conformanceWireFieldsSUB+1 {
		return fmt.Errorf("%w: %q", errConformanceWireInvalidSUB, line)
	}

	subject := fields[1]
	sid := fields[len(fields)-1]

	sub := &conformanceWireSub{client: c, subject: subject, sid: sid}
	c.mu.Lock()
	c.subs[sid] = sub
	c.mu.Unlock()

	c.server.mu.Lock()
	defer c.server.mu.Unlock()

	if c.server.bySubject[subject] == nil {
		c.server.bySubject[subject] = make(map[*conformanceWireSub]struct{})
	}

	c.server.bySubject[subject][sub] = struct{}{}

	return nil
}

func (c *conformanceWireClient) handleUnsub(line string) error {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return nil
	}

	sid := fields[1]

	c.mu.Lock()
	sub := c.subs[sid]
	delete(c.subs, sid)
	c.mu.Unlock()

	if sub == nil {
		return nil
	}

	c.server.mu.Lock()
	defer c.server.mu.Unlock()

	if subs := c.server.bySubject[sub.subject]; subs != nil {
		delete(subs, sub)

		if len(subs) == 0 {
			delete(c.server.bySubject, sub.subject)
		}
	}

	return nil
}

var (
	errConformanceWireInvalidPUB   = errors.New("invalid PUB line")
	errConformanceWireInvalidPUBSz = errors.New("invalid PUB size")
)

func (c *conformanceWireClient) handlePub(line string, reader *bufio.Reader) error {
	fields := strings.Fields(line)
	if len(fields) != conformanceWireFieldsPUB && len(fields) != conformanceWireFieldsPUB+1 {
		return fmt.Errorf("%w: %q", errConformanceWireInvalidPUB, line)
	}

	subject := fields[1]
	replyTo := ""

	sizeField := fields[len(fields)-1]
	if len(fields) == conformanceWireFieldsPUB+1 {
		replyTo = fields[2]
	}

	var size int

	_, err := fmt.Sscanf(sizeField, "%d", &size)
	if err != nil || size < 0 {
		return fmt.Errorf("%w: %q", errConformanceWireInvalidPUBSz, line)
	}

	payload := make([]byte, size)

	_, err = io.ReadFull(reader, payload)
	if err != nil {
		return fmt.Errorf("read payload: %w", err)
	}

	trailer := make([]byte, conformanceWireMaxPayload)

	_, err = io.ReadFull(reader, trailer)
	if err != nil {
		return fmt.Errorf("read trailer: %w", err)
	}

	c.server.publish(subject, replyTo, payload)

	return nil
}

func (s *conformanceWireServer) publish(subject, replyTo string, payload []byte) {
	s.mu.Lock()
	targets := make([]*conformanceWireSub, 0)

	for pattern, subs := range s.bySubject {
		if !conformanceWireSubjectMatches(pattern, subject) {
			continue
		}

		for sub := range subs {
			targets = append(targets, sub)
		}
	}
	s.mu.Unlock()

	for _, sub := range targets {
		err := sub.client.writeMsg(subject, sub.sid, replyTo, payload)
		if err != nil {
			_ = sub.client.conn.Close()
		}
	}
}

// conformanceWireSubjectMatches supports the `*` (single token) and `>`
// (trailing tokens) NATS wildcards, needed because nats.go's default
// request/reply mux subscribes to a wildcard inbox subject.
func conformanceWireSubjectMatches(pattern, subject string) bool {
	if pattern == subject {
		return true
	}

	pp := strings.Split(pattern, ".")
	sp := strings.Split(subject, ".")

	for i := range pp {
		if pp[i] == ">" {
			return i == len(pp)-1
		}

		if i >= len(sp) {
			return false
		}

		if pp[i] != "*" && pp[i] != sp[i] {
			return false
		}
	}

	return len(pp) == len(sp)
}

func (c *conformanceWireClient) writeMsg(subject, sid, replyTo string, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	var err error

	if replyTo == "" {
		_, err = fmt.Fprintf(c.conn, "MSG %s %s %d\r\n", subject, sid, len(payload))
	} else {
		_, err = fmt.Fprintf(c.conn, "MSG %s %s %s %d\r\n", subject, sid, replyTo, len(payload))
	}

	if err != nil {
		return fmt.Errorf("write msg: %w", err)
	}

	_, err = c.conn.Write(payload)
	if err != nil {
		return fmt.Errorf("write payload: %w", err)
	}

	_, err = c.conn.Write([]byte("\r\n"))
	if err != nil {
		return fmt.Errorf("write trailing newline: %w", err)
	}

	return nil
}

func (c *conformanceWireClient) writeString(s string) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	_, err := io.WriteString(c.conn, s)
	if err != nil {
		return fmt.Errorf("write string: %w", err)
	}

	return nil
}

func (c *conformanceWireClient) close() {
	c.server.mu.Lock()
	delete(c.server.clients, c)

	subs := make([]*conformanceWireSub, 0, len(c.subs))
	for _, sub := range c.subs {
		subs = append(subs, sub)
	}
	c.server.mu.Unlock()

	for _, sub := range subs {
		c.server.mu.Lock()
		if subjectSubs := c.server.bySubject[sub.subject]; subjectSubs != nil {
			delete(subjectSubs, sub)

			if len(subjectSubs) == 0 {
				delete(c.server.bySubject, sub.subject)
			}
		}
		c.server.mu.Unlock()
	}

	_ = c.conn.Close()
}
