package zephyr

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/apex/go-apex"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/savaki/zap"
)

const (
	Insert = "INSERT"
	Modify = "MODIFY"
)

type StreamRecord struct {
	Keys           map[string]*dynamodb.AttributeValue
	NewImage       map[string]*dynamodb.AttributeValue
	OldImage       map[string]*dynamodb.AttributeValue
	SequenceNumber string
	SizeBytes      int64
	StreamViewType string
}

type Record struct {
	AwsRegion      string       `json:"awsRegion"`
	Dynamodb       StreamRecord `json:"dynamodb"`
	EventID        string       `json:"eventID"`
	EventName      string       `json:"eventName"`
	EventSource    string       `json:"eventSource"`
	EventSourceARN string       `json:"eventSourceARN"`
	EventVersion   string       `json:"eventVersion"`
}

type Records struct {
	Records []Record `json:"Records"`
}

type Handler struct {
	namer     TopicNamer
	finder    TopicArnFinder
	extractor MessageExtractor
	publisher Publisher
	topicArns *cache
	writer    zap.WriteSyncer
	log       zap.Logger
}

func (h *Handler) HandlerFunc(event json.RawMessage, ctx *apex.Context) (interface{}, error) {
	defer h.log.Info("zephyr:finished")
	defer h.writer.Sync()

	var records Records
	err := json.Unmarshal(event, &records)
	if err != nil {
		h.log.Warn("zephyr:err:unmarshal", zap.Err(err))
		return nil, err
	}

	h.log.Info("zephyr:records", zap.Int("records", len(records.Records)))
	for _, record := range records.Records {
		// ---- Determine Topic Name --------------------------------------------

		topicName, err := h.namer.TopicName(record)
		if err != nil {
			h.log.Info("zephyr:err:topic_name", zap.Err(err))
		}
		if topicName == "" {
			continue
		}

		// ---- Publish Record --------------------------------------------------

		err = h.Publish(topicName, record)

		if err != nil && ErrCode(err) == "NotFound" {
			h.log.Warn("zephyr:err:topic_not_found")
			h.topicArns.Delete(topicName)
			err = h.Publish(topicName, record)
		}

		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (h *Handler) Publish(topicName string, record Record) error {
	since := time.Now()

	log := h.log.With(zap.String("name", topicName))

	// ---- Lookup Topic ARN ------------------------------------------------

	topicArn, ok := h.topicArns.Get(topicName)
	if !ok {
		arn, err := h.finder.FindTopicArn(topicName)
		if err != nil {
			log.Warn("zephyr:err:topic_arn", zap.Err(err))
			return err
		}
		topicArn = arn
		h.topicArns.Set(topicName, topicArn)
		log.Info("zephyr:topic_arn", zap.Duration("elapsed", time.Now().Sub(since)/time.Millisecond))
	}
	log = log.With(zap.String("arn", *topicArn))

	// ---- Extract Message -------------------------------------------------

	r, err := h.extractor.ExtractMessage(record)
	if err != nil {
		log.Warn("zephyr:err:extract_message", zap.Err(err))
		return err
	}

	// ---- Publish Message -------------------------------------------------

	err = h.publisher.Publish(topicArn, r)
	if err != nil {
		log.Warn("zephyr:err:publish", zap.Err(err))
		return err
	}

	log.Info("zephyr:ok", zap.Duration("elapsed", time.Now().Sub(since)/time.Millisecond))
	return nil
}

func New(opts ...Option) apex.HandlerFunc {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	cfg := &aws.Config{Region: aws.String(region)}
	client := sns.New(session.New(cfg))

	handler := &Handler{
		namer:     TopicNameFunc(topicName),
		finder:    newLookupTopicArn(client),
		extractor: ExtractMessageFunc(jsonMessage),
		publisher: newPublishFunc(client),
		writer:    zap.AddSync(ioutil.Discard),
	}

	for _, opt := range opts {
		opt(handler)
	}

	handler.topicArns = newCache()

	// setup logging
	id := strconv.FormatInt(time.Now().Unix(), 36)
	handler.log = zap.NewJSON(
		zap.Output(zap.AddSync(handler.writer)),
		zap.Append(appendTimestamp),
	).With(
		zap.String("id", id),
		zap.String("service", "zephyr"),
	)

	handler.log.Info("zephyr:started")

	return handler.HandlerFunc
}

func appendTimestamp(data []byte, t time.Time) []byte {
	data = append(data, `,"timestamp":"`...)
	data = t.UTC().AppendFormat(data, "2006-01-02T15:04:05.000Z")
	return append(data, `"`...)
}

func jsonMessage(r Record) (string, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func topicName(r Record) (string, error) {
	return "", nil
}

func newLookupTopicArn(client *sns.SNS) FindTopicArnFunc {
	return func(topicName string) (*string, error) {
		out, err := client.CreateTopic(&sns.CreateTopicInput{
			Name: aws.String(topicName),
		})
		if err != nil {
			return nil, err
		}

		return out.TopicArn, nil
	}
}

func newPublishFunc(client *sns.SNS) PublishFunc {
	return func(topicArn *string, message string) error {
		_, err := client.Publish(&sns.PublishInput{
			TopicArn: topicArn,
			Message:  aws.String(message),
		})

		return err
	}
}
