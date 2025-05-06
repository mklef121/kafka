# KAFKA

## What is event streaming?

Technically speaking, event streaming is the practice of capturing data in real-time from event sources like databases, sensors, mobile devices, cloud services, and software applications in the form of streams of events; storing these event streams durably for later retrieval; manipulating, processing, and reacting to the event streams in real-time as well as retrospectively; and routing the event streams to different destination technologies as needed. Event streaming thus ensures a continuous flow and interpretation of data so that the right information is at the right place, at the right time. 

### What can I use event streaming for?

 Event streaming is applied to a wide variety of use cases across a plethora of industries and organizations. Its many examples include:

- To process payments and financial transactions in real-time, such as in stock exchanges, banks, and insurances.
- To track and monitor cars, trucks, fleets, and shipments in real-time, such as in logistics and the automotive industry.
- To continuously capture and analyze sensor data from IoT devices or other equipment, such as in factories and wind parks.
- To collect and immediately react to customer interactions and orders, such as in retail, the hotel and travel industry, and mobile applications.
- To monitor patients in hospital care and predict changes in condition to ensure timely treatment in emergencies.
- To connect, store, and make available data produced by different divisions of a company.
- To serve as the foundation for data platforms, event-driven architectures, and microservices.

### Apache Kafka is an event streaming platform. What does that mean?

Kafka combines three key capabilities so you can implement your use cases for event streaming end-to-end with a single battle-tested solution:

- To publish (write) and subscribe to (read) streams of events, including continuous import/export of your data from other systems.
- To store streams of events durably and reliably for as long as you want.
- To process streams of events as they occur or retrospectively.

## How does Kafka work in a nutshell?
Kafka is a distributed system consisting of servers and clients that communicate via a high-performance TCP network protocol. It can be deployed on bare-metal hardware, virtual machines, and containers in on-premise as well as cloud environments. 

- **Servers:** Kafka is run as a cluster of one or more servers that can span multiple datacenters or cloud regions. Some of these servers form the storage layer, called the **brokers**. Other servers run Kafka Connect to continuously import and export data as event streams to integrate Kafka with your existing systems such as relational databases as well as other Kafka clusters. Kafka cluster is highly scalable and fault-tolerant: if any of its servers fails, the other servers will take over their work to ensure continuous operations without any data loss. 
- **Clients:** They allow you to write distributed applications and microservices that read, write, and process streams of events in parallel, at scale, and in a fault-tolerant manner even in the case of network problems or machine failures.

> A Kafka broker is a node in the Kafka cluster responsible for storing, managing, and serving data (events) in topics and partitions to producers and consumers.


### Main Concepts and Terminology
When you read or write data to Kafka, you do this in the form of events. Conceptually, an event has a key, value, timestamp, and optional metadata headers. Here's an example event:

- Event key: "Alice"
- Event value: "Made a payment of $200 to Bob"
- Event timestamp: "Jun. 25, 2020 at 2:06 p.m."

**Producers** are those client applications that publish (write) events to Kafka, and **consumers** are those that subscribe to (read and process) these events.
**Events** are organized and durably stored in **topics**. Very simplified, a **topic** is similar to a folder in a filesystem, and the events are the files in that folder. An example topic name could be "payments".

**Topics** in Kafka are always `multi-producer` and `multi-subscriber`: a topic can have zero, one, or many producers that write events to it, as well as zero, one, or many consumers that subscribe to these events.

**Events** in a topic can be read as often as needed—unlike traditional messaging systems, events are not deleted after consumption. Instead, you define for how long Kafka should retain your events through a per-topic configuration setting, after which old events will be discarded.

> Kafka's performance is effectively constant with respect to data size, so storing data for a long time is perfectly fine. 

Topics are partitioned, meaning a topic is spread over a number of **buckets** located on different Kafka brokers. This distributed placement of your data is very important for scalability because it allows client applications to both read and write the data from/to many brokers at the same time. 

> When a new event is published to a topic, it is actually appended to one of the topic's partitions. Events with the same event key (e.g., a customer or vehicle ID) are written to the same partition, and Kafka guarantees that any consumer of a given topic-partition will always read that partition's events in exactly the same order as they were written. 

## Use Cases

- **Stream Processing:** Kafka is often used to build **data processing pipelines** made up of multiple steps. 
In these pipelines:
    - **Raw data** is read from one topic (e.g., articles from RSS feeds).
    - The data is then **transformed**—such as being cleaned, enriched, or filtered.
    - The processed data is **written to new topics**, stage by stage.
    - Each stage can build on the previous one—for instance, taking cleaned articles and using them to **generate personalized recommendations**.

This step-by-step design makes it easier to organize complex data flows and scale each part independently.

- **Messaging:** Kafka is a strong alternative to traditional message brokers (e.g RabbitMQ), offering high throughput, built-in partitioning, replication, and fault-tolerance. These features make it ideal for decoupling producers and consumers, buffering messages, and handling large-scale message processing reliably.

- **Log Aggregation:** Kafka is often used instead of traditional log aggregation tools by treating logs as streams of messages rather than files. This approach enables faster processing, simplifies handling data from multiple sources, and supports distributed consumption more effectively.


> The Streams API allows transforming streams of data from input topics to output topics. (like the stream processing usecase discussed above)


## Consumer Position

- **Tracking Consumption is Crucial**: Knowing which messages have been consumed is a core performance factor in messaging systems.

- **Traditional Approach**:
  - The **broker tracks consumption**, either immediately upon delivery or after an **acknowledgement** from the consumer.
  - This prevents message loss but creates trade-offs:
    - **Immediate marking risks data loss** if the consumer crashes.
    - **Waiting for acknowledgements** avoids loss but introduces complexity: potential **duplicate consumption**, **locking**, and **state tracking** per message.
    - Handling messages that are sent but never acknowledged becomes tricky.

- **Kafka’s Simpler Model**:
  - Topics are split into **ordered partitions**.
  - Each **consumer tracks a single integer offset** per partition: the next message to consume.
  - This small amount of state makes **offset tracking cheap** and efficient.
  - Offsets are **checkpointed**, replacing complex acknowledgment logic.

- **Big Win – Rewind Capability**:
  - Kafka allows consumers to **rewind to an older offset** and **reprocess messages**.
  - This breaks the typical queue behavior but is extremely useful—e.g., after fixing a bug, you can re-consume affected data.


This model enables Kafka to balance **high performance, durability, and flexibility**, especially at scale.

## Consumer Offset Tracking
Kafka consumers need to keep track of **how far they’ve read (the offset)** in each partition, so they can **resume from the correct place** after restarts or failures. Kafka consumers keep track of which records they've processed using offsets. These offsets are stored on a special broker called the **group coordinator**, which each consumer must find before committing or fetching offsets. Consumers can either commit automatically or manage this process manually for finer control.

### **Offset Tracking and Commit**
- Each **consumer tracks the latest offset it has consumed**.
- These offsets can be **committed** (saved) to Kafka so that the consumer can **resume from that point** later.
- Kafka stores these offsets **per consumer group**, using a **special broker called the group coordinator**.

### **Group Coordinator Role**
- Every **consumer group** is assigned to a **coordinator broker**, based on the group name.
- The coordinator is responsible for **handling offset commits and fetches** for that group.

###  **How Consumers Find the Coordinator**
- A consumer sends a `FindCoordinatorRequest` to **any broker**.
- The broker responds (`FindCoordinatorResponse`) with the details of the **coordinator broker** for that group.
- All **commits and fetches** for offsets go to that specific coordinator.


###  **Handling Coordinator Changes**
- If the coordinator moves (e.g., due to broker failure), the consumer must **re-discover** the new coordinator by repeating the `FindCoordinatorRequest`.

###  **Manual vs Automatic Commits**
- Kafka allows **automatic commits** at regular intervals.
- Alternatively, consumers can **manually commit offsets** at more controlled points in the processing flow.

### What Happens During an **Offset Commit**?

1. **Consumer sends an `OffsetCommitRequest`** to its **group coordinator**.
2. The coordinator **appends the offset** to a special **compacted Kafka topic** called `__consumer_offsets`.
3. Kafka **waits for all replicas** of this topic to **acknowledge the write**.
4. Only then does Kafka **respond with a successful commit** to the consumer.
5. If replication fails within a **configurable timeout**, the commit fails, and the **consumer should retry** after a delay.


#### Why the `__consumer_offsets` Topic Matters
It’s **compacted**, so only the **latest offset per consumer partition** is retained. This keeps the topic efficient and small over time.
  

#### In-Memory Cache for Fast Fetches
The coordinator **caches offsets in memory** for **fast responses** to consumers. When a **consumer fetches offsets**, the coordinator reads from this **cache**.


#### Edge Case: Cache Not Ready

- If the coordinator is **new** (e.g., just started or became leader for a topic partition), it may **not have loaded the cache yet**.
- In that case, it returns a **`CoordinatorLoadInProgressException`**.
- The consumer must **back off and retry** the `OffsetFetchRequest`.


##  Log Compaction

Kafka supports two types of data retention:

1. **Time-based or size-based retention** — where old data is deleted after a set time or when the log reaches a certain size. This works well for event logs, metrics, or other data where each record is independent.
2. **Log compaction** — designed for use cases involving **keyed, mutable data**, like a changelog of updates to a database. Instead of deleting older records purely based on time or size, Kafka **keeps the most recent record for each unique key**. 


### Why Use Log Compaction?

- **Restoring state** after application crashes.
- **Rebuilding caches** on service restarts or during operational maintenance.
- Preserving **critical last-known values** for downstream systems.



## Partition Rebalancing in Kafka
Partition rebalancing in Kafka refers to the process where Kafka consumers adjust their assignment of partitions within a consumer group in response to changes, such as:

1. **New Consumers Joining:** A new consumer joins the group, and Kafka needs to redistribute the partitions among the active consumers.
2. **Consumers Leaving:** A consumer leaves or crashes, so Kafka reassigns its partitions to other consumers.
3. **Partition Count Changes:** If the number of partitions for a topic changes (e.g., partitions are added), Kafka will rebalance the partitions across the consumers.

Rebalancing ensures that the load is evenly distributed across the available consumers, and it is essential for efficient message consumption in a distributed system like Kafka.

### **Rebalancing Strategies**

Kafka supports different strategies for partition assignment during rebalancing. These strategies determine how Kafka will redistribute the partitions across the consumers:

1. **Range Assignor** (default):
   * Partitions are assigned to consumers in **contiguous ranges**.
   * Consumers are assigned a block of partitions sequentially (e.g., Consumer 1 gets partitions 0-2, Consumer 2 gets 3-5).
   * This is useful when partition numbers are relatively stable and evenly distributed.

2. **Round-robin Assignor**:
   * Partitions are distributed to consumers in a **round-robin** fashion, ensuring a more even load balance when the number of partitions is much higher than the number of consumers.
   * Each consumer gets partitions in a balanced manner, one by one.

3. **Sticky Assignor**:
   * Tries to **minimize the amount of partition movement** during rebalancing by sticking as much as possible to previous assignments.
   * It is a more sophisticated strategy that reduces the overhead of moving partitions unnecessarily when consumers join or leave the group.

4. **Custom Assignor**:
   * Kafka allows you to implement your own **custom partition assignor** if none of the built-in strategies meet your needs.


Example implementation in golang


```go
package brokers

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gigbanc/loyalty-and-gamification-service/interfaces"
	notificationInterfaces "github.com/gigbanc/loyalty-and-gamification-service/modules/notification/interfaces"
)

type kafkaBroker struct {
	consumerGroupConnection sarama.ConsumerGroup
	producerConnection      sarama.SyncProducer
	logger                  interfaces.ILogger
	subscribers             map[string]notificationInterfaces.MessageCallback
	consumerMutex           *sync.Mutex
	consumerInProgress      bool
}

func NewKafkerBroker(producer sarama.SyncProducer, consumerGroup sarama.ConsumerGroup, logger interfaces.ILogger) *kafkaBroker {
	return &kafkaBroker{
		producerConnection:      producer,
		consumerGroupConnection: consumerGroup,
		logger:                  logger,
		consumerMutex:           &sync.Mutex{},
		subscribers:             map[string]notificationInterfaces.MessageCallback{},
	}
}

func (kb *kafkaBroker) CreateQueue(channel, exchange string) error {
	return nil
}

func (kb *kafkaBroker) Publish(channelName, messageId, exchange string, body []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: channelName,
		Key:   sarama.StringEncoder(messageId),
		Value: sarama.ByteEncoder(body),
	}

	_, _, err := kb.producerConnection.SendMessage(msg)
	if err != nil {
		kb.logger.Errorf("cannot publish message to kafka topic %s with message id %s, error :%s", channelName, messageId, err.Error())
		return err
	}

	return nil
}

func (kb *kafkaBroker) Consume(channelName, consumerName, exchange string, callback notificationInterfaces.MessageCallback) error {

	kb.consumerMutex.Lock()
	defer kb.consumerMutex.Unlock()

	kb.subscribers[channelName] = callback

	// if this is the first call to Consume, then start the general consumer
	if !kb.consumerInProgress {
		go kb.generalConsumer()
	}

	kb.consumerInProgress = true
	return nil
}

// generalConsumer registers a one time consumer that consumes data from all topics in the kafka way.
// this was necessary since we cannot register a consumer for each topic like other broker types
func (kb *kafkaBroker) generalConsumer() {
	// sleep for some time so all subscribers are registered
	time.Sleep(10 * time.Millisecond)

	topics := []string{}
	for key := range kb.subscribers {
		topics = append(topics, key)
	}

	kb.logger.Info("about setting up kafka consumer for all topics ", topics)

	consumer := KafkaConsumer{
		logger:      kb.logger,
		ready:       make(chan bool),
		subscribers: kb.subscribers,
	}

	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := kb.consumerGroupConnection.Consume(context.Background(), topics, &consumer); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				kb.logger.Error("kafka consumer group has been close.")
				return
			}

			kb.logger.Errorf("issue consuming message in kafka consumer. error %s", err.Error())
		}

		consumer.ready = make(chan bool)
	}

}

// Consumer represents a Sarama consumer group consumer
type KafkaConsumer struct {
	ready       chan bool
	logger      interfaces.ILogger
	subscribers map[string]notificationInterfaces.MessageCallback
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (kc *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				kc.logger.Error("message channel was closed")
				return nil
			}

			// get the original callback registerd by the sunbscribing consumer
			subscriber, ok := kc.subscribers[message.Topic]

			if !ok {
				session.MarkMessage(message, "")
			}

			if err := subscriber(message.Value, string(message.Key)); err != nil {
				// TODO: implement a dead later queue to retry failed processing
				kc.logger.Errorf("message processing failed: value = %s, timestamp = %v, topic = %s, error: %s", string(message.Value), message.Timestamp, message.Topic, err.Error())
			}

			// marks a message a read by Commiting the offset using the OffsetCommitRequest to the coordinator broker
			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			kc.logger.Warnf("consumer session has ended, either due to a rebalance or disconnection. error: %#v", session.Context().Err())
			return nil
		}
	}

}


func NewKafkaConn() (sarama.SyncProducer, sarama.ConsumerGroup) {

	brokers := strings.Split(config.AppConfig.Brokers.KafkerBrokers, ",")
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		log.Fatal("Failed to start kafka producer:", err)
	}

	saraConfig := sarama.NewConfig()
	saraConfig.Consumer.Return.Errors = true
	saraConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerGroup, err := sarama.NewConsumerGroup(brokers, config.AppConfig.AppName, saraConfig)

	if err != nil {
		log.Fatal("Failed to start Kafka consumer:", err)
	}

	// the producer and consumer returnced should be closed using the "Close" method on both connections
	// when no longer in use
	return producer, consumerGroup
}

```

