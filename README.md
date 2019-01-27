# Kafkaesque message queue

After building the binaries (`broker`, `producer`, and `consumer`)

    $ broker -t topic
    $ tail -n 100 logs.txt | producer
    $ consumer --offset 324



### quotes from kafka whitepaper
> Because  of  limitations  in  existing  systems,  we  developed  a  new
>     messaging-based  log  aggregator  Kafka.  We  first  introduce  the
>     basic concepts in Kafka. A stream of messages of a particular type
>     is defined by a topic. A producer
>     can publish messages to a topic.
>     The  published  messages  are  then  stored  at  a  set  of  servers  called
>     brokers. A consumer can subscribe to one or more topics from the
>     brokers,  and  consume  the  subscribed  messages  by  pulling  data
>     from the brokers.

> Simple  storage:  Kafka  has  a  very  simple  storage  layout.  Each
>     partition of a topic corresponds to a logical log. Physically, a log
>     is  implemented  as  a  set  of  segment  files  of  approximately  the
>     same size (e.g., 1GB). Every time a producer publishes a message
>     to  a  partition,  the  broker  simply  appends  the  message  to  the  last
>     segment file. For better performance, we flush the segment files to
>     disk  only  after  a  configurable  number  of  messages  have  been
>     published  or  a  certain  amount  of  time  has  elapsed.  A  message  is
>     only exposed to the consumers after it is flushed
