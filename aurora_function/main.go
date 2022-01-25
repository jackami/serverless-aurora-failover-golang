package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"log"
	"strconv"
	"strings"
	"time"
)

var (
	sess *session.Session
	region = "ap-southeast-1"
)

func init() {
	sess, _ = session.NewSession(&aws.Config{
		Region:aws.String(region)},
	)
}

type RDSClusterDetail struct {
	EventCategories  []string  `json:"EventCategories"`
	SourceType       string    `json:"SourceType"`
	SourceArn        string    `json:"SourceArn"`
	Date             time.Time `json:"Date"`
	SourceIdentifier string    `json:"SourceIdentifier"`
	Message          string    `json:"Message"`
}

func HandleRequest(ctx context.Context, event events.CloudWatchEvent) (int, error) {
	var detail RDSClusterDetail

	if err := json.Unmarshal(event.Detail, &detail); err != nil {
		log.Println("err : ", err.Error())
	} else {
		log.Println(detail)
	}

	vs := strings.Split(detail.SourceArn, ":")
	clusterName := vs[6]
	log.Println("clusterName : " + clusterName)

	svc := rds.New(sess)

	// waiting writer failover to reader
	time.Sleep(30 * time.Second)

	readerName, err := GetReaderInstanceIdentifier(svc, clusterName)
	if err != nil {
		log.Println("err : ", err.Error())
	}

	idleCustomEndpointName, err := GetIdleCustomEndpointIdentifier(svc, clusterName)
	if err != nil {
		log.Println("err : ", err.Error())
	}

	status, err := ModifyClusterCustomEndpoint(svc, idleCustomEndpointName, readerName)
	if err != nil {
		log.Println("err : ", err.Error())
	} else {
		log.Println("status is: " + status)
	}

	for {
		status, err = GetClusterCustomEndpointStatus(svc, clusterName, idleCustomEndpointName)
		if err != nil {
			log.Println("err : ", err.Error())
		}

		time.Sleep(5 * time.Second)

		if status == "available" {
			break
		}
	}

	log.Println("-------------------- well done! --------------------")

	return 1, nil
}

func main() {
	lambda.Start(HandleRequest)
}

func GetClusterCustomEndpointStatus(svc *rds.RDS, clusterName, endpointName string) (string, error) {
	result, err := svc.DescribeDBClusterEndpoints(&rds.DescribeDBClusterEndpointsInput {
		DBClusterIdentifier: aws.String(clusterName),
	})

	if err != nil {
		log.Println("err : ", err.Error())
		return "", err
	} else {
		for _, d := range result.DBClusterEndpoints {
			if d.CustomEndpointType != nil && *d.EndpointType == "CUSTOM" && *d.DBClusterEndpointIdentifier == endpointName {
				log.Println("d.endpoint size: " + strconv.Itoa(len(d.StaticMembers)) + " d.endpoint: " +  *d.DBClusterEndpointIdentifier + " status: " + *d.Status)
				return *d.Status, nil
			}
		}
	}

	return "", nil
}

func ModifyClusterCustomEndpoint(svc *rds.RDS, customEndpointIdentifier, instanceIdentifier string) (string, error) {
	result, err := svc.ModifyDBClusterEndpoint(&rds.ModifyDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String(customEndpointIdentifier),
		EndpointType:                aws.String("ANY"),
		ExcludedMembers:             nil,
		StaticMembers:               []*string{
			aws.String(instanceIdentifier),
		},
	})

	if err != nil {
		log.Println("err : ", err.Error())
		return "", err
	} else {
		log.Println(result)
		if *result.Status == "modifying" {
			log.Println("need to hood endpoint status.")
		}
		return *result.Status, nil
	}
}

func GetIdleCustomEndpointIdentifier(svc *rds.RDS, clusterName string) (string, error) {
	result, err := svc.DescribeDBClusterEndpoints(&rds.DescribeDBClusterEndpointsInput {
		DBClusterIdentifier: aws.String(clusterName),
	})

	if err != nil {
		log.Println("err : ", err.Error())
		return "", err
	} else {
		for _, d := range result.DBClusterEndpoints {
			if d.CustomEndpointType != nil && *d.EndpointType == "CUSTOM" && len(d.StaticMembers) == 0 {
				log.Println("d.endpoint size: " + strconv.Itoa(len(d.StaticMembers)) + " d.endpoint: " +  *d.Endpoint + " status: " + *d.Status)
				return *d.DBClusterEndpointIdentifier, nil
			}
		}
	}

	return "", nil
}

func GetReaderInstanceIdentifier(svc *rds.RDS, clusterName string) (string, error) {
	result, err := svc.DescribeDBClusters(&rds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterName),
	})

	if err != nil {
		log.Println("err : ", err.Error())
		return "", err
	} else {
		for _, d := range result.DBClusters[0].DBClusterMembers {
			if *d.IsClusterWriter == false {
				log.Println("db instance identifier : " + *d.DBInstanceIdentifier)
				return *d.DBInstanceIdentifier, nil
			}
		}
	}

	return "", nil
}
