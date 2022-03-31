package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"time"

	"github.com/golang/glog"
	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

var MyCluster string
var MyMongo string
var MyNode string
var MyNamespace string
var consulHost string

// Function to watch for any changes to NxtService collection
func watchNxtDB(DBCltn *mongo.Collection) {
	var cs *mongo.ChangeStream
	var err error

	// Watch the cluster db. Retry for 5 times before bailing out of watch
	for retries := 5; retries > 0; retries-- {
		cs, err = DBCltn.Watch(context.TODO(), mongo.Pipeline{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
		if err != nil {
			// Call fatal only after the last retry otherwise, report error on continue retyring
			if retries <= 1 {
				glog.Fatalf("Not able to watch MongoDB Change notification-[err:%s]", err)
			} else {
				glog.Errorf("Not able to watch MongoDB Change notification-[err:%s] retrying... ", err)
				time.Sleep(1 * time.Second)
			}
		} else {
			glog.Info(" Database watch started ")
			break
		}
	}

	// Whenever there is a new change event, decode the event and process  it
	for cs.Next(context.TODO()) {
		var changeEvent bson.M

		err = cs.Decode(&changeEvent)
		if err != nil {
			glog.Fatal(err)
		}

		op := changeEvent["operationType"].(string)
		// Check to prevent panic error
		if op == "drop" || op == "dropDatabase" || op == "invalidate" {
			continue
		}

		//glog.Infof("ChangeEvent:%v \n", changeEvent)
		dKey := changeEvent["documentKey"].(primitive.M)

		// Get the cluster name from the id string
		id := dKey["_id"].(string)
		split := strings.Split(id, "?")
		cluster := split[0]

		// Process the events coming from other clusters only
		dns := svcInfo{}
		err = mapstructure.Decode(changeEvent["fullDocument"], &dns)
		if err != nil {
			glog.Infof("Not able to unmarshall fullDocument. Err:%s", err)
		}
		// Register or Deregister with consul if the service belongs to this
		// namespace and its from another cluster.
		// Note: Service from same cluster is taken care by the http api handler
		if cluster != MyCluster && strings.Contains(id, "."+MyNamespace+"-") {
			if op == "insert" {
				RegisterConsul(&dns)
			} else if op == "delete" {
				// Remove the clustername from if before calling DeregisterConsul
				id = strings.ReplaceAll(id, cluster+"?", "")
				DeRegisterConsul(id)
			}
		}
	}
}

// This function periodically reads the NxtServices collection entries and compares with
// consul service list and to see if any entries to be added or deleted from the DB.
// This will take care of any missed add/del service api.
func markAndSweeepDB() {

	for {
		_, svcSummary := getAllConsulServices()
		for key, val := range svcSummary {
			if !val {
				// Delete the entry from DB.
				glog.Infof("markAndSweep: Removing svc %s from DB", MyCluster+"?"+key)
				DBDeleteService(MyCluster + "?" + key)
			}
		}
		time.Sleep(5 * time.Minute)
	}
}

// Handle add service api event from consul
func addEventHandler(w http.ResponseWriter, req *http.Request) {
	var dns svcInfo

	glog.Infof(" ===========> HTTP Add Event received from consul <========")

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		glog.Infof("Request read failed")
		return
	}

	err = json.Unmarshal(body, &dns)
	if err != nil {
		glog.Infof("Error parsing json")
		return
	}
	glog.Infof("Add service to DB:%v", dns)
	DBUpdateService(&dns)
}

// Handle delete service api event from consul
func delEventHandler(w http.ResponseWriter, req *http.Request) {
	var dns svcInfo

	glog.Infof("===========> HTTP Delete Event received from consul <========")

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		glog.Infof("Request read failed")
		return
	}

	err = json.Unmarshal(body, &dns)
	if err != nil {
		glog.Infof("Error parsing json")
		return
	}
	glog.Infof("Delete service from DB:%v", dns.Meta.NextensioCluster+"?"+dns.ID)
	DBDeleteService(dns.Meta.NextensioCluster + "?" + dns.ID)
}

// Process add and delete service api calls from consul
func httpServer() {
	port := 80

	mux := http.NewServeMux()
	mux.HandleFunc("/event/add", func(w http.ResponseWriter, r *http.Request) {
		addEventHandler(w, r)
	})
	mux.HandleFunc("/event/del", func(w http.ResponseWriter, r *http.Request) {
		delEventHandler(w, r)
	})
	addr := ":" + strconv.Itoa(port)

	s2 := http2.Server{}
	server := http.Server{
		Addr: addr, Handler: h2c.NewHandler(mux, &s2),
	}
	err := server.ListenAndServe()
	if err != nil {
		glog.Fatalf("Http listen failed : ", err)
		return
	}
}

func main() {
	flag.Parse()
	MyCluster = GetEnv("MY_POD_CLUSTER", "UNKNOWN_CLUSTER")
	if MyCluster == "UNKNOWN_CLUSTER" {
		glog.Fatal("Uknown cluster name")
	}
	MyMongo = GetEnv("MY_MONGO_URI", "UNKNOWN_MONGO")
	if MyMongo == "UNKNOWN_MONGO" {
		glog.Fatal("Unknown Mongo URI")
	}
	MyNode = GetEnv("MY_NODE_NAME", "UNKNOWN_NODE")
	if MyCluster == "UNKNOWN_NODE" {
		glog.Fatal("Uknown node name")
	}
	MyNamespace = GetEnv("MY_POD_NAMESPACE", "UNKNOWN_NODE")
	if MyCluster == "UNKNOWN_NODE" {
		glog.Fatal("Uknown node name")
	}

	consulHost = MyNode + ".node.consul:8500"
	glog.Infof("====> MyNode:%s consulHost:%s <======", MyNode, consulHost)
	for {
		if DBConnect() {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// Get all services created by other clusters associated with our namespace
	// and update the consul
	err, svcInfo := DBFindAllServicesOfTenant(MyNamespace)
	if err != nil {
		glog.Infof("Not able to get namespace service info from DB")
	} else {
		glog.Infof("Register with consul all services associated with this tenant namespace from other clusters\n")
		for _, dns := range svcInfo {
			if dns.Meta.NextensioCluster != MyCluster {
				RegisterConsul(&dns)
			}
		}
	}

	// Start the mark and Sweep process which will get all the services registered on this
	// cluster and update the NxtServices collection DB as needed.
	go markAndSweeepDB()

	// Strat watching the NxtServices collection DB for any new updates
	go watchNxtDB(serviceCltn)

	// Start the httpServer to process consul add and delete api service handlers
	httpServer()

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt)
	signal.Notify(term, syscall.SIGTERM)
	select {
	case <-term:
	}
}
