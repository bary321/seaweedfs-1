package util

import (
	"net"

	"github.com/bary321/seaweedfs-1/weed/glog"
)

func DetectedHostAddress() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		glog.V(0).Infof("failed to detect ip address: %v", err)
		return ""
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return "localhost"
}
