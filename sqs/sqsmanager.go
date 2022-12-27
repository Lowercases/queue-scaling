package sqs

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

// Whatever's implementing the control, likely an ECS Manager.
type SQSControlManager interface {
	SetB() chan float64
	Beta() (uint, error)
}

// Implements the control.Manager interface
type SQSManager struct {
	queue string

	// Url for SQS API
	queueUrl *string

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
	sqs := awssqs.New(sess)

	t := time.Now().UTC()

	if m.queueUrl == nil {
		gquo, err := sqs.GetQueueUrl(&awssqs.GetQueueUrlInput{
			QueueName: aws.String(m.queue),
		})
		if err != nil {
			return fmt.Errorf("Error querying SQS: %s", err)
		}
		m.queueUrl = new(string)
		*m.queueUrl = *gquo.QueueUrl
	}

	// These could be got from CloudWatch, but it's best to get them from SQS
	// given that a sleepy queue (one that hasn't got a message for six hours)
	// will be considered "asleep" by AWS and will stop updating CloudWatch.
	// In order to wake it up, the SQS API must be hit, so we might as well
	// query these from SQS directly.
	gqao, err := sqs.GetQueueAttributes(&awssqs.GetQueueAttributesInput{
		QueueUrl: m.queueUrl,
		AttributeNames: []*string{
			aws.String("ApproximateNumberOfMessagesNotVisible"),
			aws.String("ApproximateNumberOfMessages"),
		},
	})
	if err != nil {
		return fmt.Errorf("Error querying SQS: %s", err)
	}

	q, err := strconv.Atoi(*gqao.Attributes["ApproximateNumberOfMessages"])
	if err != nil {
		return fmt.Errorf("Error parsing SQS response: %s", err)
	}
	w, err := strconv.Atoi(*gqao.Attributes["ApproximateNumberOfMessagesNotVisible"])
	if err != nil {
		return fmt.Errorf("Error parsing SQS response: %s", err)
	}

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
		},
		StartTime: aws.Time(t.Add(-time.Minute * 3)),
		EndTime:   aws.Time(t),
	})
	if err != nil {
		return fmt.Errorf("Error querying Cloudwatch: %s", err)
	}

	// FIXME parametrise in function of the fucking array we're sending
	if len(gmdo.MetricDataResults) != 2 {
		return fmt.Errorf("Error querying Cloudwatch: expected 2 results, got %d", len(gmdo.MetricDataResults))
	}

	var dx, dy float64
	for _, results := range gmdo.MetricDataResults {
		switch *results.Id {
		case "sent":
			dx, err = parseNextToLastMetric(results)
		case "deleted":
			dy, err = parseNextToLastMetric(results)
		default:
			err = fmt.Errorf("Unknown metric %s", *results.Id)
		}
		if err != nil {
			return fmt.Errorf("Error parsing metrics: %s", err)
		}
	}

	// It's ok to use data per minute
	m.dx, m.dy = dx, dy
	m.q, m.xmy = uint(q), uint(q + w)

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
