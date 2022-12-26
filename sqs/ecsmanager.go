package sqs

import (
	"fmt"
	"log"
	"math"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ecs"
)

type ECSManager struct {
	setB             chan float64
	cluster, service string
	ecs              *ecs.ECS
}

func NewECSManager(cluster, service string) *ECSManager {
	sess := session.Must(session.NewSession())

	m := &ECSManager{
		setB:    make(chan float64),
		cluster: cluster,
		service: service,
		ecs:     ecs.New(sess),
	}

	go m.run()

	return m

}

func (m *ECSManager) run() {
	open := true
	for open {
		var b float64
		select {
		case b, open = <-m.setB:
			m.updateB(int64(math.Round(b)))
		}
	}
}

func (m *ECSManager) SetB() chan float64 {
	return m.setB
}

func (m *ECSManager) Beta() (uint, error) {
	dso, err := m.ecs.DescribeServices(&ecs.DescribeServicesInput{
		Cluster:  aws.String(m.cluster),
		Services: []*string{aws.String(m.service)},
	})
	if err != nil {
		return 0, err
	}
	if len(dso.Services) != 1 {
		return 0, fmt.Errorf("Service %s not found in cluster %s",
			m.service, m.cluster)
	}
	srv := dso.Services[0]

	return uint(*srv.RunningCount), nil

}

func (m *ECSManager) updateB(b int64) {
	_, err := m.ecs.UpdateService(&ecs.UpdateServiceInput{
		Cluster:      aws.String(m.cluster),
		Service:      aws.String(m.service),
		DesiredCount: aws.Int64(b),
	})
	if err != nil {
		log.Printf("Error updating service %s in cluster %s: %s",
			m.service, m.cluster, err)
	}

}
