package finago

import (
	"github.com/coreos/go-etcd/etcd"
	"testing"
)

func TestService(t *testing.T) {
	machines := []string{}
	etcd.NewClient(machines)

	//test announcement on etcd
}
