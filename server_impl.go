// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
    "strconv"
    "fmt"
    "net"
    "bufio"
    "strings"
)


type keyValueServer struct {
	caller1 chan int
	caller2 chan int
	clients_W []chan string
}

type key_value struct{
	key string
	val []byte
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	server := new(keyValueServer)
	return server
}



func (kvs *keyValueServer) Start(port int) error {
	init_db()
	port1 := ":"+ strconv.Itoa(port)
	ln, err := net.Listen("tcp", port1)
	key_chan := make(chan *key_value)
	val_chan := make(chan *key_value)
	key_val_chan := make(chan *key_value)
	go writeHandler(key_val_chan, key_chan, val_chan)
	caller1 := make( chan int)
	caller2 := make( chan int)
	kvs.caller1 = caller1
	kvs.caller2 = caller2
	adder := make(chan int)
	go counter(adder,caller1,caller2)


	if err != nil {
		return err
	}

	go handler(kvs,ln,key_val_chan,key_chan,val_chan,adder)
	return err
	

}
func handler(kvs *keyValueServer, ln net.Listener,key_val_chan chan *key_value,key_chan chan *key_value, val_chan chan *key_value, adder chan int) {

	for {
		conn, err := ln.Accept()
		// handle successful connections concurrently
		if err != nil {
			fmt.Printf("Couldn't accept a client connection: %s\n", err)
		} else {
			adder<-1
			go handleConnection(conn, kvs,key_val_chan,key_chan,val_chan)
		}
		
	}
	
}


func handleConnection(conn net.Conn,kvs *keyValueServer,key_val_chan chan *key_value,key_chan chan *key_value, val_chan chan *key_value) {

	// clean up once the connection closes
	defer conn.Close()

	r := bufio.NewReader(conn)
	client := make(chan string)
	go sender(client,conn)
	kvs.clients_W = append(kvs.clients_W,client)
	for {
		msg, err := r.ReadString('\n')
		if err != nil {
		}else if msg [:4] == "get," {
			key := msg[4:len(msg)-1]
			key_val:= new(key_value)
			key_val.key = key
			key_chan<-key_val
			val := (<-val_chan).val 
			value:= key + "," + string(val)
			// fmt.Printf(value)
			for i := range kvs.clients_W {
				kvs.clients_W[i] <-(value)
			}
			
		}else if msg[:4]=="put," {
			command:= strings.Split(msg,",")
			val := []byte(command[2]) 
			key_val := new(key_value)
			key_val.val = val
			key_val.key = command[1]
			key_val_chan <- key_val
			
		}
	}
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
}

func (kvs *keyValueServer) Count() int {

	kvs.caller1<-1;
	caller2 := kvs.caller2
	return <-caller2
}

// TODO: add additional methods/functions below!
func writeHandler(key_val_chan <-chan *key_value, key_chan <-chan *key_value, val_chan chan<- *key_value){
	for {
		select{
		case key_val:= <-key_val_chan:
			put(key_val.key,key_val.val)

		case key_val:= <-key_chan:
			val := get(key_val.key)
			key_val.val= val
			val_chan<-key_val
		}
		

	}
}

func sender(send chan string, client net.Conn) {

	for {
		mesg:= <-send
		client.Write([]byte(mesg))
	}
}

func counter(adder chan int, caller1 chan int, caller2 chan int){

	count:=0;
	for {
		select{
		case <-adder:
			count++

		case <-caller1:
			caller2 <- count
		}
	}

}