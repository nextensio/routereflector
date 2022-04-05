package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
)

var myClient = &http.Client{Timeout: 10 * time.Second}

type serviceDetail []struct {
	ID              string `json:"ID"`
	Node            string `json:"Node"`
	Address         string `json:"Address"`
	Datacenter      string `json:"Datacenter"`
	TaggedAddresses struct {
		Lan     string `json:"lan"`
		LanIpv4 string `json:"lan_ipv4"`
		Wan     string `json:"wan"`
		WanIpv4 string `json:"wan_ipv4"`
	} `json:"TaggedAddresses"`
	NodeMeta struct {
		ConsulNetworkSegment string `json:"consul-network-segment"`
	} `json:"NodeMeta"`
	ServiceKind            string        `json:"ServiceKind"`
	ServiceID              string        `json:"ServiceID"`
	ServiceName            string        `json:"ServiceName"`
	ServiceTags            []interface{} `json:"ServiceTags"`
	ServiceAddress         string        `json:"ServiceAddress"`
	ServiceTaggedAddresses struct {
		LanIpv4 struct {
			Address string `json:"Address"`
			Port    int    `json:"Port"`
		} `json:"lan_ipv4"`
		WanIpv4 struct {
			Address string `json:"Address"`
			Port    int    `json:"Port"`
		} `json:"wan_ipv4"`
	} `json:"ServiceTaggedAddresses"`
	ServiceWeights struct {
		Passing int `json:"Passing"`
		Warning int `json:"Warning"`
	} `json:"ServiceWeights"`
	ServiceMeta struct {
		NextensioCluster string `json:"NextensioCluster"`
		NextensioPod     string `json:"NextensioPod"`
	} `json:"ServiceMeta"`
	ServicePort              int  `json:"ServicePort"`
	ServiceEnableTagOverride bool `json:"ServiceEnableTagOverride"`
	ServiceProxy             struct {
		MeshGateway struct {
		} `json:"MeshGateway"`
		Expose struct {
		} `json:"Expose"`
	} `json:"ServiceProxy"`
	ServiceConnect struct {
	} `json:"ServiceConnect"`
	CreateIndex int `json:"CreateIndex"`
	ModifyIndex int `json:"ModifyIndex"`
}

/*
 * Register DNS svcInfo for the service
 */
func RegisterConsul(dns *svcInfo) (e error) {

	url := "http://" + MyNode + ".node.consul:8500/v1/agent/service/register"
	glog.Infof("RegisterConsul : DNS:%v\n\n", dns)

	js, e := json.Marshal(dns)
	if e != nil {
		glog.Errorf("Consul: failed to make make json at %s, error %s", url, e)
		return e
	}
	r, e := http.NewRequest("PUT", url, bytes.NewReader(js))
	if e != nil {
		glog.Errorf("Consul: failed to make http request at %s, error %s", url, e)
		return e
	}
	r.Header.Add("Content-Type", "application/json")
	r.Header.Add("Accept-Charset", "UTF-8")
	resp, e := myClient.Do(r)
	if e == nil && resp.StatusCode == 200 {
		glog.Infof("Consul: registered via http PUT at %s", url)
		glog.Infof("Consul: registered service json %s", js)
	} else {
		status := -1
		if resp != nil {
			status = resp.StatusCode
		}
		if e == nil {
			e = fmt.Errorf("bad http response %d", status)
		}
		glog.Errorf("Consul: failed to register via http PUT at %s, error %s, %d", url, e, status)
		glog.Errorf("Consul: failed to register service json %s", js)
		return e
	}
	return nil
}

/*
 * DeRegister DNS entry and PodIP:Podname key:value pair for the service
 */
func DeRegisterConsul(id string) (e error) {
	var err error

	url := "http://" + MyNode + ".node.consul:8500/v1/agent/service/deregister/" + id
	glog.Infof("DeRegisterConsul : URL:%s\n\n", url)

	r, e := http.NewRequest("PUT", url, nil)
	if e != nil {
		glog.Errorf("Consul: deregister failed to make http request at %s, error %s", url, e)
		return e
	}
	resp, e := myClient.Do(r)
	if e != nil || resp.StatusCode != 200 {
		status := -1
		if resp != nil {
			status = resp.StatusCode
		}
		glog.Errorf("Consul: http PUT of nil at %s failed err %s, code %s %d", url, e, status)
		// Well, keep going and delete all the services even if this one failed.
		// If the service is really going away from the pod, the health check will
		// eventually fail and remove this service in approx 1.5 minutes
	}
	return err
}

/*
 * This function does:
 *  - Gets all service entries from consul
 *  - Gets all DB service entries corresponding the tenant ns and cluster
 *  - Creates a map of service entries and marks it add or del to indicate if
 *  - the service needs to be added/deleted from DB.
 */
func getAllConsulServices() (map[string]bool, error) {
	services := make(map[string][]string)
	svcSummary := make(map[string]bool)

	// Get all existing services in consul
	url := "http://" + MyNode + ".node.consul:8500/v1/catalog/services"
	resp, err := myClient.Get(url)
	if err != nil {
		glog.Errorf("Error doing http Get. %v", err)
		return svcSummary, err
	}
	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Not able to read response data. %v", err)
		return svcSummary, err
	}

	err = json.Unmarshal([]byte(respData), &services)
	if err != nil {
		glog.Errorf("Not able to unmarshall the response. %v", err)
	}
	resp.Body.Close()

	svcs, err := DBFindAllServicesOfTenantInCluster(MyNamespace)
	if err != nil {
		glog.Error("Not able to get tenant service info from DB", err)
	} else {
		for _, dns := range svcs {
			svcSummary[dns.ID] = false
		}
	}

	// Now get the individual service info to update the DB
	// TODO: This will become a scale issue soon. Consul unfortunately has no concept of
	// seggregating services across "namespaces". So this RR pod which runs in each tenant's
	// namespace will fetch the entire catalog containing ALL tenants and then filter out
	// one tenant (ourselves). We NEED to find some way of organizing data in consul per-tenant
	// One saving grace can be that we can keep the RR pod on a seperate machine/node by itself
	// and that seperate machine will have a consul agent of its own. So we are isolating high
	// CPU and network usage to just one node because the RR to consul-agent is a local http access
	url = "http://" + MyNode + ".node.consul:8500/v1/catalog/service"
	for key, _ := range services {
		var sInfo serviceDetail
		var dns svcInfo

		if key == "consul" {
			continue
		}
		resp, err := myClient.Get(url + "/" + key)
		if err != nil {
			glog.Errorf("Error doing http Get. %v", err)
			continue
		}
		// Check for status 200 and take appropriate action
		respData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			glog.Infof("Not able to read service %s response data. %v", key, err)
			continue
		}
		err = json.Unmarshal([]byte(respData), &sInfo)
		if err != nil {
			glog.Infof("Not able to unmarshall the response. %v", err)
			continue
		}
		dns.ID = sInfo[0].ServiceID
		dns.Name = sInfo[0].ServiceName
		dns.Address = sInfo[0].ServiceAddress
		dns.Meta.NextensioCluster = sInfo[0].ServiceMeta.NextensioCluster
		dns.Meta.NextensioPod = sInfo[0].ServiceMeta.NextensioPod

		// Only update the service corresponding to this cluster and namespace in DB
		if dns.Meta.NextensioCluster == MyCluster && strings.HasSuffix(dns.Name, MyNamespace) {
			if _, ok := svcSummary[dns.ID]; ok {
				// Service exists in DB.
				svcSummary[dns.ID] = true
			} else {
				// Service doesn't exist in DB
				glog.Infof("markAndSweep: Add service %s to DB", dns.ID)
				for {
					err = DBUpdateService(&dns)
					if err == nil {
						break
					}
					glog.Error("M&S: DBUpdate error", err)
					time.Sleep(2 * time.Second)
				}
				svcSummary[dns.ID] = false
			}
		}
		defer resp.Body.Close()
	}
	return svcSummary, err
}
