package listener

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"os"
	"strconv"
)

var svc = getSQSInstance()
var qURL = getQueueUrl(svc, "ttv-search-asset-clicks")

func ReadUserMessages() chan *sqs.Message {

	chnMessages := make(chan *sqs.Message, 10)
	go pollSqs(svc, qURL, chnMessages)

	return chnMessages

}

func DeleteMessage(msg *sqs.Message) {
	delete_params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qURL),
		ReceiptHandle: msg.ReceiptHandle,
	}

	_, err := svc.DeleteMessage(delete_params)
	if err != nil {
		log.Println(err)
	}
	fmt.Printf("[Delete message] \nMessage ID: %s has beed deleted.\n\n", *msg.MessageId)
}

func SendMessagesInLoop(prefix string) {

	for i := 0; i < 100; i++ {
		send_params := &sqs.SendMessageInput{
			MessageBody:  aws.String(prefix + "Hello Me " + strconv.Itoa(i)), // Required
			QueueUrl:     aws.String(qURL),
			DelaySeconds: aws.Int64(3),
		}
		send_resp, err := svc.SendMessage(send_params)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("[Send message] \n%v \n\n", send_resp)
	}

}

func getSQSInstance() *sqs.SQS {

	cmdArgs := os.Args[1:]
	accessKeyId := cmdArgs[0]
	accessSecret := cmdArgs[1]

	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(endpoints.ApSoutheast2RegionID),
		Credentials: credentials.NewStaticCredentials(accessKeyId, accessSecret, ""),
	}))
	return sqs.New(sess)
}

func getQueueUrl(svc *sqs.SQS, qName string) string {
	result, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(qName),
	})
	if err != nil {
		fmt.Println("Error", err)
		os.Exit(1)
	}
	return *result.QueueUrl
}

func readFromQueue(svc *sqs.SQS, qURL string) {
	result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &qURL,
		MaxNumberOfMessages: aws.Int64(10),
		VisibilityTimeout:   aws.Int64(60), // 60 seconds
		WaitTimeSeconds:     aws.Int64(0),
	})

	if err != nil {
		fmt.Println("Error", err)
		return
	}
	if len(result.Messages) == 0 {
		fmt.Println("Received no messages")
		return
	}
	fmt.Printf("Success: %+v\n", result.Messages)
}

func pollSqs(svc *sqs.SQS, qURL string, chn chan<- *sqs.Message) {

	for {
		output, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            &qURL,
			MaxNumberOfMessages: aws.Int64(10),
			WaitTimeSeconds:     aws.Int64(0),
		})

		if err != nil {
			log.Fatal("failed to fetch sqs message %v", err)
		}

		for _, message := range output.Messages {
			chn <- message
		}

	}

}
