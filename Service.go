package finago

import (
	"github.com/coreos/go-etcd/etcd"
	"strings"
	"time"
)

// provide service registry and service discovery functions
// service will expire,  keep alive

// every service maintains a connection pool?

type Service struct {
	Name string
	Dst  string
	Port string

	config  Config
	eclient *etcd.Client // etcd client
	stop    chan bool    // use to stop heartbeat
}

type Config struct {
	HartBeatInterval time.Duration
	TTL              time.Duration
}

const (
	DEFAULT_HEARTBEAT_INTERVAL = 1000 * time.Millisecond
	MINIMUM_HEARTBEAT_INTERVAL = 50 * time.Millisecond // avoid too fast beacon
)

func NewSerivce(serviceName, ipaddr, port string, etcdClient *etcd.Client) *Service {
	config := Config{
		HartBeatInterval: DEFAULT_HEARTBEAT_INTERVAL,
		TTL:              DEFAULT_HEARTBEAT_INTERVAL * 3}

	return &Service{serviceName, ipaddr, port, config, etcdClient, make(chan bool)}
}

// key format /serviceName/IP+port
func (s *Service) getKeyName() string {
	return "/" + s.Name + "/" + s.Dst + ":" + s.Port
}

// announce service to etcd
// the TTL is 2*
func (s *Service) Announce() (err error) {
	// Create a service key with TTL

	_, err = s.eclient.Set(s.getKeyName(), "1", uint64(s.config.TTL.Seconds()))

	if err != nil {
		return err
	}

	// start heartbeat
	tick := time.Tick(s.config.HartBeatInterval)
	go s.heartBeat(tick)

	return
}

// remove key from etcd
func (s *Service) Close() {

	s.stop <- true
}

func (s *Service) heartBeat(tick <-chan time.Time) {
	// reset TTL
	for {
		select {
		case <-tick:
			// reset ttl and reset
			s.eclient.Set(s.getKeyName(), "1", uint64(s.config.TTL.Seconds()))
		case <-s.stop:
			return
		default:
			time.Sleep(MINIMUM_HEARTBEAT_INTERVAL)

		}
	}

}

func (s *Service) SetHeatBeatInterval(millisecond int) {
	s.config.HartBeatInterval = time.Duration(millisecond) * time.Millisecond
}

//ToDo: return dest for connectionPool
// return a list of services
func GetAvailableServices(serviceName string, etcdClient *etcd.Client) (serviceList []*Service, err error) {

	// list dir
	response, err := etcdClient.Get(serviceName, true, false)
	if err != nil {
		return
	}
	for _, node := range response.Node.Nodes {
		res := strings.Split(node.Key, ":") // dstination and port
		ipaddr, port := res[0], res[1]
		s := NewSerivce(serviceName, ipaddr, port, etcdClient)
		serviceList = append(serviceList, s)
	}

	return
}

func getDestsByService(serviceName string, etcdClient *etcd.Client) (dsts []string, err error) {
	serviceLst, err := GetAvailableServices(serviceName, etcdClient)

	if err != nil {
		return
	}

	for _, service := range serviceLst {
		dsts = append(dsts, service.Dst+":"+service.Port)
	}

	return
}
