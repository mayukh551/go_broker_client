package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
)

type Broker struct {
	ClientId      string
	Host          string
	Port          int
	Conn          net.Conn
	Subscribers   map[string]string // topic to subscriber ID mapping
	Username      string
	Password      string
	EnableStorage bool
}

type brokerCommand struct {
	Command string `json:"command"`
	Payload any    `json:"payload"`
	Topic   string `json:"topic"`
	SubId   string `jsonL:"subId"`
}

type AuthCommand struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	IsAuth        bool
	EnableStorage bool
}

type authAckCommand struct {
	IsAuthenticated bool `json:"isAuthenticated"`
}

type AckCommand struct {
	Status int    `json:"status"` // 1 -> Ok, 0 -> Failure
	Reason string `json:"reason"`
}

func getIP(b *Broker) string {
	return net.JoinHostPort(b.Host, fmt.Sprintf("%d", b.Port))
}

func (b *Broker) Connect() error {

	conn, err := net.Dial("tcp", "ec2-15-134-204-186.ap-southeast-2.compute.amazonaws.com")

	if err != nil {
		println("Broker Connect Error", err)
		return fmt.Errorf("failed to connect to Broker")
	}

	reader := bufio.NewScanner(conn)
	println("Reading data from broker")

	// listenning for connection ack
	for reader.Scan() {
		msg := reader.Text()
		println("message received :", msg)
		var data string
		json.Unmarshal([]byte(msg), &data)
		if data == "ok" {
			break
		}
	}

	println("Connected to broker server")

	data, err := json.Marshal(AuthCommand{Username: b.Username, Password: b.Password, IsAuth: true, EnableStorage: b.EnableStorage})

	if err != nil {
		return fmt.Errorf("failed to send your auth credentials")
	}

	println("Sending Auth")
	_, err = conn.Write(append(data, '\n'))

	if err != nil {
		return err
	}

	reader = bufio.NewScanner(conn)

	for reader.Scan() {
		msg := reader.Text()
		println("Recieved ack data", msg)
		var authAck string
		json.Unmarshal([]byte(msg), &authAck)

		if authAck == "ACCEPTED" {
			println("Connected to broker", b.Host, b.ClientId, b.Port)
			b.Conn = conn
			return nil
		} else {
			return fmt.Errorf("invalid creds. Failed to connect")
		}
	}

	return nil

}

func (b *Broker) Publish(topic string, payload any) error {

	conn := b.Conn

	if conn == nil {
		panic("connect before publish")
	}

	jsonData, err := json.Marshal(brokerCommand{Command: "publish", Payload: payload, Topic: topic})

	if err != nil {
		return fmt.Errorf("failed to stringify brokerCommand, %w", err)
	}

	_, err = conn.Write(append(jsonData, '\n'))

	reader := bufio.NewScanner(conn)

	for reader.Scan() {
		msg := reader.Text()
		var ack AckCommand
		err := json.Unmarshal([]byte(msg), &ack)
		if err != nil {
			return fmt.Errorf("failed to unmarshal acknowledgement from broker, %w", err)
		}

		if ack.Status == 1 {
			return nil
		} else {
			return fmt.Errorf("%s", ack.Reason)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to write brokerCommand json data into connection, %w", err)
	}

	return nil
}

func (b *Broker) Subscribe(topic string) error {

	id := uuid.New().String() // create new subscriber id

	b.Subscribers[topic] = id

	fmt.Println("b.Conn before reading", b.Conn)

	conn := b.Conn

	if conn == nil {
		panic("connect before subscribe")
	}

	jsonData, err := json.Marshal(brokerCommand{Command: "subscribe", Topic: topic, SubId: id})

	println("Subscribe Json data to send", jsonData)

	if err != nil {
		return fmt.Errorf("failed to stringify brokerCommand, %w", err)
	}

	_, err = conn.Write(append(jsonData, '\n'))

	if err != nil {
		return fmt.Errorf("failed to write brokerCommand json data into connection, %w", err)
	}

	reader := bufio.NewScanner(conn)

	for reader.Scan() {
		println("Waiting...")
		msg := reader.Text()
		var ack AckCommand
		err := json.Unmarshal([]byte(msg), &ack)

		if err != nil {
			return fmt.Errorf("failed to unmarshal acknowledge from broker, %w", err)
		}

		if ack.Status == 1 {
			return nil
		} else {
			return fmt.Errorf("failed to subscribe to topic")
		}
	}

	return nil
}

func (b *Broker) Consume(topic string) (any, error) {

	if b.Conn == nil {
		panic("connect before consuming")
	}

	subID := b.Subscribers[topic]

	jsonData, err := json.Marshal(brokerCommand{Command: "consume", Topic: topic, SubId: subID})

	if err != nil {
		return nil, fmt.Errorf("failed to marshal json data for consume brokerCommand, %w", err)
	}

	b.Conn.Write(append(jsonData, '\n'))

	scanner := bufio.NewScanner(b.Conn)

	for scanner.Scan() {
		data := scanner.Bytes()

		var msg any
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, fmt.Errorf("failed to decode json: %w", err)
		} else {
			fmt.Printf("Received: %#v\n", msg)
			if msg == "EMPTY_BROKER_ERROR" {
				msg = nil
			}
			return msg, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return nil, nil
}

func str(s any) string {
	return fmt.Sprint(s)
}

func New(host string, port int, clientId string, username string, password string, enableStorage bool) Broker {
	id := ""
	if clientId != "" {
		id = clientId
	} else {
		id = "broker_" + str(time.Now().Unix())
	}

	if host == "" {
		host = "localhost"
	}

	if port == 0 {
		port = 8080
	}

	return Broker{ClientId: id, Host: host, Port: port, Subscribers: make(map[string]string), Username: username, Password: password, EnableStorage: enableStorage}
}
