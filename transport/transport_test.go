package transport

import (
	"go.uber.org/zap"
	"net"
	"testing"
)

func TestHelloMessage(t *testing.T) {
	var hello helloMessage
	hello.ClusterID = 123456789
	hello.NodeID = 987654321

	data := hello.encode()

	var hello2 helloMessage
	hello2.decode(data)

	if hello2 != hello {
		t.Fatal(hello2)
	}
}

func TestNewTransport(t *testing.T) {
	type member struct {
		ID    uint64
		Addrs []string
	}
	members := []member{
		{
			ID:    1,
			Addrs: []string{"127.0.0.1:5000"},
		},
		{
			ID:    2,
			Addrs: []string{"127.0.0.1:5001"},
		},
		/*{
			ID:    3,
			Addrs: []string{"127.0.0.1:5002"},
		},*/
	}

	var transports []*Transport
	for _, member := range members {
		opt := DefaultOptions()
		opt.Logger = opt.Logger.With(zap.Uint64("member", member.ID))
		_, opt.Port, _ = net.SplitHostPort(member.Addrs[0])
		trans := NewTransport(opt)
		go func() {
			if err := trans.Run(); err != nil {
				t.Fatal(err)
			}
		}()
		for _, member := range members {
			trans.AddPeer(member.ID, member.Addrs)
		}
		transports = append(transports, trans)
	}

	select {}

}
