==
logk
==

Program to tail a file and send each line or multi lines  to a Kafka topic.  Written in go.

Example usage:

conf.json

```json
{

  "hostName" : "192.168.199.212" ,
  "kafkaServers": ["192.168.199.212:9092"] ,
  "topic" : "test" ,
  "batchSize" : 5 ,
  "tails" :[
    {
      "filePath" : "/Users/wens/access.log" ,
      "tags" : {
        "index" : "true" ,
        "indexName" :  "ddd"
      }

    }
  ]

}

```

```shell
logk -conf conf.json
```

kafka message format

```
192.168.100.1 - - [17/Sep/2015:15:09:20 +0800] "GET / HTTP/1.1" 200 151 "-" "curl/7.35.0"|#|nginx|#|/var/log/nginx/access.log|#|192.168.100.101|#|1442473760696|#|{"index":"true","indexName":"accesslog"}
```