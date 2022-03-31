package main

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/golang/glog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var dbClient *mongo.Client
var unitTesting bool

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

func DBConnect() bool {
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

	serviceDB = dbClient.Database("NxtDB")
	serviceCltn = serviceDB.Collection("NxtServices")

	return true
}

func DBCheckError(err error) {
	// If there is network or timeout error from the server, sit in a loop until
	// its cleared as we can't do much until the DB is accessible again.
	for mongo.IsNetworkError(err) || mongo.IsTimeout(err) {
		glog.Infof("DB Error : %v", err)
		time.Sleep(2 * time.Second)
	}
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
	if unitTesting {
		mongoErr := GetEnv("TEST_MONGO_ERR", "NOT_TEST")
		if mongoErr == "true" {
			glog.Error("Mongo UT error")
			return errors.New("Mongo  unit test error")
		}
	}

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
		DBCheckError(err.Err())
		return err.Err()
	}
	glog.Infof("%s service record added to DB successfully", dns.ID)
	return nil
}

func DBDeleteService(id string) error {
	if unitTesting {
		mongoErr := GetEnv("TEST_MONGO_ERR", "NOT_TEST")
		if mongoErr == "true" {
			glog.Error("Mongo UT error")
			return errors.New("Mongo unit test error")
		}
	}

	_, err := serviceCltn.DeleteOne(
		context.TODO(),
		bson.M{"_id": id},
	)

	if err != nil {
		DBCheckError(err)
		return err
	}
	glog.Infof("%s service record deleted from DB successfully", id)

	return nil
}

// Return the svcInfo associated with the id.
func DBFindService(id string) (error, *svcInfo) {
	if unitTesting {
		mongoErr := GetEnv("TEST_MONGO_ERR", "NOT_TEST")
		if mongoErr == "true" {
			glog.Error("Mongo UT error")
			return errors.New("Mongo unit test error"), nil
		}
	}
	var svc svcInfo

	err := serviceCltn.FindOne(
		context.TODO(),
		bson.M{"_id": id},
	).Decode(&svc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}

	if err != nil {
		DBCheckError(err)
		return err, nil
	}
	return nil, &svc
}

// Find all service documents corresponding to a tenant namespace
func DBFindAllServicesOfTenant(tenant string) (error, []svcInfo) {

	if unitTesting {
		mongoErr := GetEnv("TEST_MONGO_ERR", "NOT_TEST")
		if mongoErr == "true" {
			glog.Error("Mongo UT error")
			return errors.New("Mongo  unit test error"), nil
		}
	}

	var svcs []svcInfo
	cursor, err := serviceCltn.Find(
		context.TODO(),
		bson.M{"Name": primitive.Regex{Pattern: tenant, Options: ""}})

	if err == mongo.ErrNoDocuments {
		return nil, nil
	}

	if err != nil {
		DBCheckError(err)
		return err, nil
	}

	err = cursor.All(context.TODO(), &svcs)
	if err != nil {
		DBCheckError(err)
		return err, nil
	}
	return nil, svcs
}

// Find all service documents corresponding to tenant ns on this cluster
func DBFindAllServicesOfTenantInCluster(tenant string) (error, []svcInfo) {

	if unitTesting {
		mongoErr := GetEnv("TEST_MONGO_ERR", "NOT_TEST")
		if mongoErr == "true" {
			glog.Error("Mongo UT error")
			return errors.New("Mongo  unit test error"), nil
		}
	}

	var svcs []svcInfo

	cursor, err := serviceCltn.Find(
		context.TODO(),
		bson.M{"Name": primitive.Regex{Pattern: tenant, Options: ""}, "Meta.NextensioCluster": MyCluster})

	if err == mongo.ErrNoDocuments {
		return nil, nil
	}

	if err != nil {
		DBCheckError(err)
		return err, nil
	}

	err = cursor.All(context.TODO(), &svcs)
	if err != nil {
		DBCheckError(err)
		return err, nil
	}
	return nil, svcs
}
