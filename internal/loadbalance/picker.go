package loadbalance

import (
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"strings"
	"sync"
	"sync/atomic"
)

func init() {
	balancer.Register(
		base.NewBalancerBuilder(Name, &Picker{}, base.Config{}))
}

type Picker struct {
	mu        sync.Mutex
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

var _ base.PickerBuilder = (*Picker)(nil)

func (p *Picker) Build(info base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()

	var followers []balancer.SubConn
	for sc, scInfo := range info.ReadySCs {
		isLeader := scInfo.Address.Attributes.Value("is_leader").(bool)
		if isLeader {
			p.leader = sc
			continue
		}
		followers = append(followers, sc)
	}
	p.followers = followers
	return p
}

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var result balancer.PickResult
	if strings.Contains(info.FullMethodName, "Produce") ||
		len(p.followers) == 0 {
		result.SubConn = p.leader
	} else if strings.Contains(info.FullMethodName, "Consume") {
		result.SubConn = p.nextFollower()
	}
	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}
	return result, nil
}

func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))
	ln := uint64(len(p.followers))
	return p.followers[int(cur%ln)]
}
