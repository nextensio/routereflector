package main

import (
	"context"
	"os"

	"github.com/golang/glog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var dbClient *mongo.Client

// Collections specific to this cluster for tracking users and services
var serviceDB *mongo.Database
var serviceCltn *mongo.Collection

func GetEnv(key string, defaultValue string) string {
	v := os.Getenv(key)
	if v == "" {
		v = defaultValue
	}
	return v
}

func DBConnect(namespace string) bool {
	var err error
	dbClient, err = mongo.NewClient(options.Client().ApplyURI(MyMongo))
	if err != nil {
		glog.Error("Database client create failed")
		return false
	}

	err = dbClient.Connect(context.TODO())
	if err != nil {
		glog.Error("Database connect failed")
		return false
	}
	err = dbClient.Ping(context.TODO(), readpref.Primary())
	if err != nil {
		glog.Errorf("Database ping error - %s", err)
		return false
	}

	serviceDB = dbClient.Database("Nxt-" + namespace + "-DB")
	serviceCltn = serviceDB.Collection("NxtServices")

	return true
}

// Json structure for registering consul service
type Meta struct {
	NextensioCluster string `bson:"NextensioCluster" json:"NextensioCluster"`
	NextensioPod     string `bson:"NextensioPod" json:"NextensioPod"`
}

type svcInfo struct {
	ID      string `bson:"ID" json:"ID"`
	Name    string `bson:"Name" json:"Name"`
	Address string `bson:"Address" json:"Address"`
	Meta    Meta   `bson:"Meta" json:"Meta"`
}

func DBUpdateService(dns *svcInfo) error {
	// The upsert option asks the DB to add if one is not found
	upsert := true
	after := options.After
	opt := options.FindOneAndUpdateOptions{
		ReturnDocument: &after,
		Upsert:         &upsert,
	}
	err := serviceCltn.FindOneAndUpdate(
		context.TODO(),
		bson.M{"_id": dns.Meta.NextensioCluster + "?" + dns.ID},
		bson.D{
			{"$set", dns},
		},
		&opt,
	)

	if err.Err() != nil {
		return err.Err()
	}
	glog.Infof("%s service record added to DB successfully", dns.ID)
	return nil
}

func DBDeleteService(id string) error {
	_, err := serviceCltn.DeleteOne(
		context.TODO(),
		bson.M{"_id": id},
	)

	if err != nil {
		return err
	}
	glog.Infof("%s service record deleted from DB successfully", id)

	return nil
}

// Return the svcInfo associated with the id.
func DBFindService(id string) (*svcInfo, error) {
	var svc svcInfo

	err := serviceCltn.FindOne(
		context.TODO(),
		bson.M{"_id": id},
	).Decode(&svc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}
	return &svc, nil
}

// Find all service documents corresponding to a tenant namespace
func DBFindAllServicesOfTenant(tenant string) ([]svcInfo, error) {
	var svcs []svcInfo
	cursor, err := serviceCltn.Find(context.TODO(), bson.M{})

	if err == mongo.ErrNoDocuments {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	err = cursor.All(context.TODO(), &svcs)
	if err != nil {
		return nil, err
	}
	return svcs, nil
}

// Find all service documents corresponding to tenant ns on this cluster
func DBFindAllServicesOfTenantInCluster(tenant string) ([]svcInfo, error) {
	var svcs []svcInfo

	// TODO: Will this regex become heavy on mongo ? If it does, we can organize
	// the collection per cluster. Lets just keep it simple for now, we will do
	// that if we see this becomes an issue
	cursor, err := serviceCltn.Find(
		context.TODO(),
		bson.M{"Name": primitive.Regex{Pattern: tenant, Options: ""}, "Meta.NextensioCluster": MyCluster})

	if err == mongo.ErrNoDocuments {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	err = cursor.All(context.TODO(), &svcs)
	if err != nil {
		return nil, err
	}
	return svcs, nil
}
