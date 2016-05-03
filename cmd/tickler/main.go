package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/codegangsta/cli"
	"github.com/savaki/zephyr/topicbyevent"
)

type Options struct {
	Region       string
	Table        string
	Type         string
	HashKey      string
	HashKeyValue string
	Interval     int
}

var opts Options

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{"region", "us-east-1", "aws region", "AWS_REGION", &opts.Region},
		cli.StringFlag{"table", "", "name of table to udpate", "", &opts.Table},
		cli.StringFlag{"type", "state", "type of function", "", &opts.Type},
		cli.StringFlag{"hk", "", "hash key", "", &opts.HashKey},
		cli.StringFlag{"hkv", "", "hash key value", "", &opts.HashKeyValue},
		cli.IntFlag{"interval", 0, "repeat interval; 0 for no repeat", "", &opts.Interval},
	}
	app.Action = Run
	app.Run(os.Args)
}

func check(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func Run(c *cli.Context) {
	cfg := &aws.Config{Region: aws.String(opts.Region)}
	client := dynamodb.New(session.New(cfg))

	if opts.Interval == 0 {
		RunOnce(client)

	} else {
		ticker := time.NewTicker(time.Second * time.Duration(opts.Interval))
		for {
			RunOnce(client)
			<-ticker.C
		}
	}
}

func RunOnce(client *dynamodb.DynamoDB) {
	item := map[string]*dynamodb.AttributeValue{}
	state := "state" + strconv.Itoa(int(time.Now().UnixNano()%5))
	if opts.Type == "event" {
		item[":attr"] = topicbyevent.Marshal("matt-users-"+state, "blah")

	} else if opts.Type == "state" {
		fmt.Fprintln(os.Stderr, state)
		v, err := dynamodbattribute.ConvertTo(state)
		check(err)
		item[":attr"] = v

	} else {
		check(fmt.Errorf("Invalid type, %v", opts.Type))
	}

	hashValue, err := dynamodbattribute.ConvertTo(opts.HashKeyValue)
	check(err)

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "HashKey: %v\n", opts.HashKey)
	fmt.Fprintf(os.Stderr, "HashKeyValue: %v\n", opts.HashKeyValue)
	fmt.Fprintf(os.Stderr, "State: %v\n", state)

	_, err = client.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(opts.Table),
		Key: map[string]*dynamodb.AttributeValue{
			opts.HashKey: hashValue,
		},
		UpdateExpression: aws.String("SET #attr = :attr"),
		ExpressionAttributeNames: map[string]*string{
			"#attr": aws.String(opts.Type),
		},
		ExpressionAttributeValues: item,
	})
	check(err)

}
