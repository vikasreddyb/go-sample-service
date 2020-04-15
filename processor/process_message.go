package processor

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/sqs"
	"sync"
	"time"
	"vrb.snippets/go/sample/listener"
)

type Job struct {
	CreatedAt time.Time
	sqsmsg    *sqs.Message
}

type Result struct {
	job     Job
	message *string
}

var jobChannel = make(chan Job, 20)
var resultChannel = make(chan Result, 100)

func Process() {

	//go listener.SendMessagesInLoop("Alpha")
	//go listener.SendMessagesInLoop("Beta")

	chnMessages := listener.ReadUserMessages()

	go printMessages()
	go allocate(chnMessages)
	createWorkerPool(10)

}

func parseMessage(sqsmsg *sqs.Message, serial int) *string {
	//fmt.Printf("parseMessage() by %v : %+v\n", serial, *sqsmsg.Body)
	return sqsmsg.Body
}

func worker(wg *sync.WaitGroup, serial int) {
	fmt.Printf("Worker %v created \n", serial)
	for job := range jobChannel {
		fmt.Printf("Worker %v processing: %+v\n", serial, *job.sqsmsg.MessageId)
		output := Result{job, parseMessage(job.sqsmsg, serial)}
		resultChannel <- output
		listener.DeleteMessage(job.sqsmsg)
	}
	wg.Done()
	fmt.Printf("Worker %v done \n", serial)
}

func createWorkerPool(noOfWorkers int) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg, i+1)
	}
	wg.Wait()
	close(resultChannel)
}

func allocate(chnMessages chan *sqs.Message) {
	for message := range chnMessages {
		job := Job{time.Now(), message}
		jobChannel <- job
	}
	close(jobChannel)
}

func printMessages() {
	for message := range resultChannel {
		fmt.Printf("Message: %+v\n", *message.message)
	}
}
