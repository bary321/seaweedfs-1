package weed_server

import (
	"fmt"
	"net/http"

	"google.golang.org/grpc"

	"github.com/bary321/seaweedfs-1/weed/stats"
	"github.com/bary321/seaweedfs-1/weed/util"

	"github.com/bary321/seaweedfs-1/weed/glog"
	"github.com/bary321/seaweedfs-1/weed/security"
	"github.com/bary321/seaweedfs-1/weed/storage"
)

type VolumeServer struct {
	SeedMasterNodes []string
	currentMaster   string
	pulseSeconds    int
	dataCenter      string
	rack            string
	store           *storage.Store
	guard           *security.Guard
	grpcDialOption  grpc.DialOption

	needleMapKind           storage.NeedleMapType
	FixJpgOrientation       bool
	ReadRedirect            bool
	compactionBytePerSecond int64
	metricsAddress          string
	metricsIntervalSec      int
	fileSizeLimitBytes      int64
	isHeartbeating          bool
	stopChan                chan bool
}

func NewVolumeServer(adminMux, publicMux *http.ServeMux, ip string,
	port int, publicUrl string,
	folders []string, maxCounts []int, minFreeSpacePercents []float32,
	needleMapKind storage.NeedleMapType,
	masterNodes []string, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string,
	fixJpgOrientation bool,
	readRedirect bool,
	compactionMBPerSecond int,
	fileSizeLimitMB int,
) *VolumeServer {

	v := util.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
	enableUiAccess := v.GetBool("access.ui")

	readSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	vs := &VolumeServer{
		pulseSeconds:            pulseSeconds,
		dataCenter:              dataCenter,
		rack:                    rack,
		needleMapKind:           needleMapKind,
		FixJpgOrientation:       fixJpgOrientation,
		ReadRedirect:            readRedirect,
		grpcDialOption:          security.LoadClientTLS(util.GetViper(), "grpc.volume"),
		compactionBytePerSecond: int64(compactionMBPerSecond) * 1024 * 1024,
		fileSizeLimitBytes:      int64(fileSizeLimitMB) * 1024 * 1024,
		isHeartbeating:          true,
		stopChan:                make(chan bool),
	}
	vs.SeedMasterNodes = masterNodes

	vs.checkWithMaster()

	vs.store = storage.NewStore(vs.grpcDialOption, port, ip, publicUrl, folders, maxCounts, minFreeSpacePercents, vs.needleMapKind)
	vs.guard = security.NewGuard(whiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	handleStaticResources(adminMux)
	adminMux.HandleFunc("/status", vs.statusHandler)
	if signingKey == "" || enableUiAccess {
		// only expose the volume server details for safe environments
		adminMux.HandleFunc("/ui/index.html", vs.uiStatusHandler)
		/*
			adminMux.HandleFunc("/stats/counter", vs.guard.WhiteList(statsCounterHandler))
			adminMux.HandleFunc("/stats/memory", vs.guard.WhiteList(statsMemoryHandler))
			adminMux.HandleFunc("/stats/disk", vs.guard.WhiteList(vs.statsDiskHandler))
		*/
	}
	adminMux.HandleFunc("/", vs.privateStoreHandler)
	if publicMux != adminMux {
		// separated admin and public port
		handleStaticResources(publicMux)
		publicMux.HandleFunc("/", vs.publicReadOnlyHandler)
	}

	go vs.heartbeat()
	glog.V(0).Infof("volume server sends metrics to %s every %d seconds", vs.metricsAddress, vs.metricsIntervalSec)
	hostAddress := fmt.Sprintf("%s:%d", ip, port)
	go stats.LoopPushingMetric("volumeServer", hostAddress, stats.VolumeServerGather, vs.metricsAddress, vs.metricsIntervalSec)

	return vs
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}
