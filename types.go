package zephyr

// ---- TopicName ---------------------------------------------------------------

type TopicNameFunc func(record Record) (string, error)

func (fn TopicNameFunc) TopicName(record Record) (string, error) {
	return fn(record)
}

type TopicNamer interface {
	TopicName(record Record) (string, error)
}

// ---- TopicArnFinder ----------------------------------------------------------

type FindTopicArnFunc func(topicName string) (*string, error)

func (fn FindTopicArnFunc) FindTopicArn(topicName string) (*string, error) {
	return fn(topicName)
}

type TopicArnFinder interface {
	FindTopicArn(topicName string) (*string, error)
}

// ---- MessageExtractor --------------------------------------------------------

type ExtractMessageFunc func(record Record) (string, error)

func (fn ExtractMessageFunc) ExtractMessage(record Record) (string, error) {
	return fn(record)
}

type MessageExtractor interface {
	ExtractMessage(record Record) (string, error)
}

// ---- Publisher ---------------------------------------------------------------

type PublishFunc func(topicArn *string, message string) error

func (fn PublishFunc) Publish(topicArn *string, message string) error {
	return fn(topicArn, message)
}

type Publisher interface {
	Publish(topicArn *string, message string) error
}
