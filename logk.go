package main

import (
	"fmt"
	"time"
	"flag"
	"os"
	"io/ioutil"
	"os/signal"
	"encoding/json"
	"github.com/ActiveState/tail"
	s "github.com/Shopify/sarama"
	home "github.com/mitchellh/go-homedir"

	"path"
	"bufio"
	"io"
	"sync"
	"bytes"
	"strings"
	"strconv"
)


type  Conf struct {
	HostName     string `json:"hostName"`
	KafkaServers []string `json:"kafkaServers"`
	Topic        string `json:"topic"`
	BatchSize    int `json:"batchSize"`
	Tails        []*Tail `json:"tails"`

}

type Tail struct {
	LogPath   string `json:"logPath"`
	AppName   string `json:"appName"`
	Tags      map[string]string  `json:"tags"`
	Separator string `json:"separator"`
}

type Record struct {
	Content   string
	Tags      map[string]string
	AppName   string
	LogPath   string
	HostName  string
	Timestamp int64
	Offset    int64
}



var confPath string
var debug bool
var runtimeInfo map[string]int64;
var waiGroup sync.WaitGroup
var closeChan chan bool
var recordChan chan *Record


func init() {

	flag.StringVar(&confPath, "conf", "/etc/logk/conf.json", "-conf conf.json")
	flag.BoolVar(&debug, "debug", true, "-debug true ")
	runtimeInfo = make(map[string]int64)
	closeChan = make(chan bool)
	recordChan = make(chan *Record, 10)
}

func main() {

	//confPath = "/home/wens/go/src/logk/conf.json"

	confFile, err := os.Open(confPath)

	if err != nil {
		if err == os.ErrNotExist {
			fmt.Printf("Can not find conf : %s ", confPath)
			os.Exit(1)
		}
		fmt.Printf("Read conf fail : %s", confPath)
		os.Exit(1)
	}

	bb, err := ioutil.ReadAll(confFile)

	if err != nil {
		panic(err)
	}
	conf := new(Conf)
	err = json.Unmarshal(bb, conf)
	if err != nil {
		panic(err)
	}

	if len( conf.KafkaServers ) == 0 {
		fmt.Println("Please config kafka servers ")
		os.Exit(1)
	}

	if len( conf.Topic ) == 0 {
		fmt.Println("Please config topic ")
		os.Exit(1)
	}

	if len( conf.Tails ) == 0 {
		fmt.Println("Please config tails ")
		os.Exit(1)
	}

	for _ , tl := range conf.Tails {

		if len( tl.AppName ) == 0 {
			fmt.Println("Please config appName ")
			os.Exit(1)
		}

		if len( tl.LogPath ) == 0 {
			fmt.Println("Please config logPath ")
			os.Exit(1)
		}
	}

	if conf.BatchSize <= 0 {
		conf.BatchSize = 10
	}

	if len(conf.HostName) == 0 {
		conf.HostName, _ = os.Hostname()
	}

	kconf := s.NewConfig()
	kconf.Net.DialTimeout = time.Second * 30
	kconf.Producer.MaxMessageBytes = 2 * 1024
	kconf.Producer.Retry.Max = 100
	kconf.Producer.Retry.Backoff = time.Second * 10
	kconf.Producer.Timeout = time.Second * 6
	kconf.Producer.Flush.Frequency = time.Second * 5
	kconf.Producer.Flush.Messages = conf.BatchSize;


	kclient, err := s.NewClient( conf.KafkaServers, kconf)

	if err != nil {
		panic(err)
	}
	defer kclient.Close()

	if debug {
		fmt.Printf("connect to kafka success . \n")
	}


	producer, err := s.NewAsyncProducerFromClient(kclient)


	if err != nil {
		panic(err)
	}
	defer producer.Close()

	waiGroup.Add(1)
	go func() {
		defer waiGroup.Done()
		buf := bytes.NewBuffer(make([]byte,0) )
		for {

			select {
			case e := <-producer.Errors():
				fmt.Println(e.Err)
				time.Sleep(time.Second * 2)
				producer.Input() <- e.Msg;
			case <-closeChan:
				return
			default:
			}

			select {
			case r := <-recordChan:
				//
				buf.WriteString(r.Content)
				buf.WriteString("|#|")
				buf.WriteString(r.AppName)
				buf.WriteString("|#|")
				buf.WriteString(r.LogPath)
				buf.WriteString("|#|")
				buf.WriteString(r.HostName)
				buf.WriteString("|#|")
				buf.WriteString(strconv.FormatInt(r.Timestamp, 10 ))
				buf.WriteString("|#|")
				bb, err := json.Marshal(r.Tags)
				if err != nil {
					if debug {
						fmt.Println(err)
					}
					continue
				}
				buf.Write(bb)

				runtimeInfo[r.LogPath] = r.Offset
				producer.Input() <- &s.ProducerMessage{
					Topic: conf.Topic,
					Value: s.ByteEncoder(buf.Bytes()),
					Key:s.StringEncoder(r.AppName),
				}
				buf.Reset()
			case <-closeChan:
				return

			}
		}
	}()



	recoverRuntimeInfo()
	defer saveRuntimeInfo()

	for _, tailConf := range conf.Tails {
		waiGroup.Add(1)
		go tailFile(conf, tailConf, conf.Topic)
	}



	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
	close(closeChan)
	fmt.Println("Waiting for exit.")
	waiGroup.Wait()
	fmt.Println("Exit ok.")

}

func saveRuntimeInfo() {

	homeDir, _ := home.Dir()
	parentPath := path.Join(homeDir, ".logk")

	_, err := os.Stat(parentPath)
	if err != nil {
		os.Mkdir(parentPath, 0777)
	}

	file, err := os.OpenFile(path.Join(parentPath, "runtimeinfo"), os.O_CREATE | os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for k, v := range runtimeInfo {
		file.Write([]byte(fmt.Sprintf("%s %d\n", k, v)))
	}
}

func recoverRuntimeInfo() {
	homeDir, _ := home.Dir()
	file, err := os.OpenFile(path.Join(homeDir, ".logk", "runtimeinfo"), os.O_RDONLY, 0666)
	if err != nil {
		return
	}

	defer file.Close()
	buf := bufio.NewReader(file)

	for {
		line, err := buf.ReadString('\n')

		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		var f string;
		var p int64;
		fmt.Sscanf(line, "%s %d", &f, &p)

		stat , err := os.Stat(f)

		if err != nil {
			p = int64(0)
		}else{
			if stat.Size() < p {
				p = 0
			}
		}

		runtimeInfo[f] = p
	}
}

func tailFile(conf *Conf, tailConf *Tail, topic string) {
	defer waiGroup.Done()
	t, err := tail.TailFile(tailConf.LogPath, tail.Config{Follow: true, ReOpen: true, Location : &tail.SeekInfo{
		Whence : os.SEEK_SET,
		Offset:runtimeInfo[tailConf.LogPath],
	}})

	if err != nil {
		panic(err)
	}

	if debug {
		fmt.Printf("begin tail file : %s \n", tailConf.LogPath)
	}


	buf := bytes.NewBuffer(make([]byte, 2 * 1024))
	buf.Reset()

	for {
		select {
		case line := <-t.Lines:
			if debug {
				fmt.Printf("%s : %s \n", tailConf.LogPath, line.Text)
			}

			if len(tailConf.Separator) == 0 {
				offset, _ := t.Tell()
				recordChan <- &Record{
					Content: line.Text ,
					Tags:tailConf.Tags,
					Timestamp:line.Time.UnixNano()/1000000,
					AppName:tailConf.AppName,
					LogPath:tailConf.LogPath,
					HostName:conf.HostName,
					Offset:offset,
				}
			}else {
				if buf.Len() >= 2 * 1024 || strings.HasPrefix(line.Text, tailConf.Separator) && buf.Len() > 0 {
					offset, _ := t.Tell()
					recordChan <- &Record{
						Content:buf.String(),
						Tags:tailConf.Tags,
						Timestamp:line.Time.UnixNano() /1000000 ,
						AppName:tailConf.AppName,
						LogPath:tailConf.LogPath,
						HostName:conf.HostName,
						Offset:offset - int64( len(line.Text)) ,
					}
					buf.Reset()
				}

				if strings.HasPrefix(line.Text, tailConf.Separator) {
					line.Text = strings.TrimPrefix(line.Text, tailConf.Separator)
				}

				buf.WriteString(line.Text + "\r\n")
			}
		case <-closeChan:
			return
		}
	}

}
