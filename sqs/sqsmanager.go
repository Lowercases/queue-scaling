package sqs

import (
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

// Whatever's implementing the control, likely an ECS Manager.
type SQSControlManager interface {
	SetB() chan float64
	Beta() (uint, error)
}

// Implements the control.Manager interface
type SQSManager struct {
	queue string

	// Stats
	dx, dy float64
	xmy, q uint

	updatePeriod time.Duration

	// Stats have errored, panic if queried
	err error

	control SQSControlManager
}

func NewSQSManager(queue string, updatePeriod time.Duration, control SQSControlManager) *SQSManager {
	m := &SQSManager{
		queue:   queue,
		control: control,
	}

	m.err = m.updateStats()
	go m.run(updatePeriod)

	return m
}

func (m *SQSManager) run(updatePeriod time.Duration) {
	for {
		m.err = m.updateStats()
		time.Sleep(updatePeriod)
	}
}

func (m *SQSManager) SetB() chan float64 {
	return m.control.SetB()
}

func (m *SQSManager) Beta() uint {
	beta, err := m.control.Beta()
	if err != nil {
		panic(fmt.Sprintf("Error querying ECS manager for %s: %s", m.queue, err))
	}
	return beta
}

func (m *SQSManager) MuP() (float64, bool) {
	return 0, false
}

func (m *SQSManager) updateStats() error {
	sess := session.Must(session.NewSession())
	cw := cloudwatch.New(sess)

	t := time.Now().UTC()

	gmdo, err := cw.GetMetricData(&cloudwatch.GetMetricDataInput{
		MetricDataQueries: []*cloudwatch.MetricDataQuery{
			{
				Id: aws.String("sent"),
				MetricStat: &cloudwatch.MetricStat{
					Metric: &cloudwatch.Metric{
						Namespace:  aws.String("AWS/SQS"),
						MetricName: aws.String("NumberOfMessagesSent"),
						Dimensions: []*cloudwatch.Dimension{{
							Name:  aws.String("QueueName"),
							Value: aws.String(m.queue),
						}},
					},
					Period: aws.Int64(60),
					Stat:   aws.String("Sum"),
				},
			},
			{
				Id: aws.String("deleted"),
				MetricStat: &cloudwatch.MetricStat{
					Metric: &cloudwatch.Metric{
						Namespace:  aws.String("AWS/SQS"),
						MetricName: aws.String("NumberOfMessagesDeleted"),
						Dimensions: []*cloudwatch.Dimension{{
							Name:  aws.String("QueueName"),
							Value: aws.String(m.queue),
						}},
					},
					Period: aws.Int64(60),
					Stat:   aws.String("Sum"),
				},
			},
			{
				Id: aws.String("visible"),
				MetricStat: &cloudwatch.MetricStat{
					Metric: &cloudwatch.Metric{
						Namespace:  aws.String("AWS/SQS"),
						MetricName: aws.String("ApproximateNumberOfMessagesVisible"),
						Dimensions: []*cloudwatch.Dimension{{
							Name:  aws.String("QueueName"),
							Value: aws.String(m.queue),
						}},
					},
					Period: aws.Int64(60),
					Stat:   aws.String("Sum"),
				},
			},
			{
				Id: aws.String("notvisible"),
				MetricStat: &cloudwatch.MetricStat{
					Metric: &cloudwatch.Metric{
						Namespace:  aws.String("AWS/SQS"),
						MetricName: aws.String("ApproximateNumberOfMessagesNotVisible"),
						Dimensions: []*cloudwatch.Dimension{{
							Name:  aws.String("QueueName"),
							Value: aws.String(m.queue),
						}},
					},
					Period: aws.Int64(60),
					Stat:   aws.String("Sum"),
				},
			},
		},
		StartTime: aws.Time(t.Add(-time.Minute * 3)),
		EndTime:   aws.Time(t),
		//NumberOfMessagesDeleted
	})
	if err != nil {
		return fmt.Errorf("Error querying Cloudwatch: %s", err)
	}

	// FIXME parametrise in function of the fucking array we're sending
	if len(gmdo.MetricDataResults) != 4 {
		return fmt.Errorf("Error querying Cloudwatch: expected 4 results, got %d", len(gmdo.MetricDataResults))
	}

	var dx, dy float64
	var q, w float64
	for _, results := range gmdo.MetricDataResults {
		switch *results.Id {
		case "sent":
			dx, err = parseNextToLastMetric(results)
		case "deleted":
			dy, err = parseNextToLastMetric(results)
		case "visible":
			q, err = parseLastMetric(results)
		case "notvisible":
			w, err = parseLastMetric(results)
		default:
			err = fmt.Errorf("Unknown metric %s", *results.Id)
		}
		if err != nil {
			return fmt.Errorf("Error parsing metrics: %s", err)
		}
	}

	// It's ok to use data per minute
	m.dx, m.dy = dx, dy
	m.q, m.xmy = uint(math.Round(q)), uint(math.Round(q+w))

	return nil

}

func (m *SQSManager) DXY(unit time.Duration) (float64, float64) {
	if m.err != nil {
		panic(m.err)
	}

	factor := float64(time.Minute / unit)
	return m.dx / factor, m.dy / factor

}

func (m *SQSManager) Q() uint {
	if m.err != nil {
		panic(m.err)
	}

	return m.q

}

func (m *SQSManager) XmY() uint {
	if m.err != nil {
		panic(m.err)
	}

	return m.xmy
}

func parseNextToLastMetric(mr *cloudwatch.MetricDataResult) (float64, error) {
	// At least two results are needed, since the last result might be incomplete.
	// Search for the second to last metric.
	if len(mr.Timestamps) < 2 {
		return 0, fmt.Errorf("Expected at least two metrics, got %d", len(mr.Timestamps))
	}

	var last, second int
	if mr.Timestamps[0].After(*mr.Timestamps[1]) {
		last = 0
		second = 1
	} else {
		last = 1
		second = 0
	}
	for i := 2; i < len(mr.Timestamps); i++ {
		if mr.Timestamps[i].After(*mr.Timestamps[last]) {
			second = last
			last = i
		} else if mr.Timestamps[i].After(*mr.Timestamps[second]) {
			second = i
		}
	}

	return *mr.Values[second], nil

}

func parseLastMetric(mr *cloudwatch.MetricDataResult) (float64, error) {
	if len(mr.Timestamps) < 1 {
		return 0, fmt.Errorf("Expected at least one metrics, got %d", len(mr.Timestamps))
	}

	last := 0
	for i := 1; i < len(mr.Timestamps); i++ {
		if mr.Timestamps[i].After(*mr.Timestamps[last]) {
			last = i
		}
	}

	return *mr.Values[last], nil

}
