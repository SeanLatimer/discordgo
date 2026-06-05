package discordgo

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestOpenTimeoutWaitingForHello(t *testing.T) {
	server := httptest.NewServer(httpTestHandler(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r, nil, 1024, 1024)
		if err != nil {
			t.Fatalf("upgrade failed: %v", err)
		}
		defer conn.Close()
		time.Sleep(200 * time.Millisecond)
	}))
	defer server.Close()

	oldHelloTimeout := gatewayHelloTimeout
	gatewayHelloTimeout = 50 * time.Millisecond
	defer func() { gatewayHelloTimeout = oldHelloTimeout }()

	s, err := New("Bot test")
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	s.gateway = websocketURL(server.URL)
	s.ShouldReconnectOnError = false

	err = s.Open()
	if err == nil {
		t.Fatal("Open() error = nil, want timeout")
	}

	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("Open() error = %v, want timeout net.Error", err)
	}

	if s.wsConn != nil {
		t.Fatal("Open() left wsConn set after timeout")
	}
}

func TestCloseSendsNormalCloseFrame(t *testing.T) {
	closeCodes := make(chan int, 1)
	server := httptest.NewServer(httpTestHandler(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r, nil, 1024, 1024)
		if err != nil {
			t.Fatalf("upgrade failed: %v", err)
		}
		defer conn.Close()

		conn.SetCloseHandler(func(code int, text string) error {
			closeCodes <- code
			return nil
		})

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	s := mustSessionWithConn(t, websocketURL(server.URL))

	if err := s.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	select {
	case code := <-closeCodes:
		if code != websocket.CloseNormalClosure {
			t.Fatalf("close code = %d, want %d", code, websocket.CloseNormalClosure)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive close frame")
	}
}

func TestCloseGatewayConnectionAbortSkipsCloseFrame(t *testing.T) {
	closeCodes := make(chan int, 1)
	server := httptest.NewServer(httpTestHandler(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r, nil, 1024, 1024)
		if err != nil {
			t.Fatalf("upgrade failed: %v", err)
		}
		defer conn.Close()

		conn.SetCloseHandler(func(code int, text string) error {
			closeCodes <- code
			return nil
		})

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	s := mustSessionWithConn(t, websocketURL(server.URL))
	s.DataReady = true

	if err := s.closeGatewayConnection(nil, false); err != nil {
		t.Fatalf("closeGatewayConnection() failed: %v", err)
	}

	if s.wsConn != nil {
		t.Fatal("closeGatewayConnection() left wsConn set")
	}
	if s.listening != nil {
		t.Fatal("closeGatewayConnection() left listening set")
	}
	if s.DataReady {
		t.Fatal("closeGatewayConnection() left DataReady true")
	}

	select {
	case code := <-closeCodes:
		t.Fatalf("received unexpected close frame with code %d", code)
	case <-time.After(200 * time.Millisecond):
	}
}

type httpTestHandler func(http.ResponseWriter, *http.Request)

func (h httpTestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h(w, r)
}

func websocketURL(url string) string {
	return "ws" + strings.TrimPrefix(url, "http")
}

func mustSessionWithConn(t *testing.T, url string) *Session {
	t.Helper()

	s, err := New("Bot test")
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	conn, _, err := s.Dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	s.wsConn = conn
	s.listening = make(chan interface{})
	return s
}
