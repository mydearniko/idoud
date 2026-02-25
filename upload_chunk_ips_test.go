package main

import (
	"net"
	"testing"
)

func TestNormalizeRemoteIP(t *testing.T) {
	tests := []struct {
		name string
		addr net.Addr
		want string
	}{
		{
			name: "ipv4 tcp",
			addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.8"), Port: 443},
			want: "203.0.113.8",
		},
		{
			name: "ipv6 tcp",
			addr: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443},
			want: "2001:db8::1",
		},
		{
			name: "non ip addr",
			addr: &net.UnixAddr{Name: "/tmp/test.sock", Net: "unix"},
			want: "",
		},
		{
			name: "nil",
			addr: nil,
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeRemoteIP(tc.addr)
			if got != tc.want {
				t.Fatalf("normalizeRemoteIP()=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestChunkOriginIPSetListSortedUnique(t *testing.T) {
	s := &chunkOriginIPSet{}
	s.add("203.0.113.8")
	s.add("2001:db8::1")
	s.add("203.0.113.8")

	got := s.list()
	if len(got) != 2 {
		t.Fatalf("len(list)=%d, want 2", len(got))
	}
	if got[0] != "2001:db8::1" || got[1] != "203.0.113.8" {
		t.Fatalf("list=%v, want [2001:db8::1 203.0.113.8]", got)
	}
}
