#!/usr/bin/env python
"""
Usage:
  script.py <kafkaHost> <kafkaPort> <tcpHost> <tcpPort> <topic> <logTopic>
"""

from docopt import docopt
from datetime import datetime
import time
import asyncio
from kafka import KafkaProducer

class TCPServerProducer:
  MAX_CONNECTION = 1
  LOG_FORMAT ="{} UTC_TS\t"\
              "{}"
              
  def __init__(self,kafka_host,kafka_port,
                    tcp_host,tcp_port,
                    topic,log_topic):
    self.kafka_host = kafka_host
    self.kafka_port = kafka_port
    self.tcp_host = tcp_host
    self.tcp_port = tcp_port
    self.topic = topic
    self.log_topic = log_topic
    self.producer = KafkaProducer(bootstrap_servers=["{}:{}".format(kafka_host,kafka_port)])
    self.connections = set()
    
  def log(self,msg):
    self.producer.send( self.log_topic,
                        self.LOG_FORMAT.format( datetime.now().timestamp(),
                                                msg
                                                ) \
                            .encode()
                        )

  def run(self):
    self.log("running")
    asyncio.run(self._async_run())
    
  async def _async_run(self):
    tcpServer = await asyncio.start_server(self.connection_handler,self.tcp_host,self.tcp_port)
    await tcpServer.serve_forever()

  async def connection_handler(self,reader,writer):
    addr = str(writer.get_extra_info("socket").getpeername())
    # A new connection, but we can accept no more
    if addr not in self.connections and len(self.connections)>=self.MAX_CONNECTION:
      self.log("refused "+addr)
      writer.write(b"Connection refused.")
      writer.close()
      return
    # An existing connection
    elif addr in self.connections:
      pass
    # A new connection, but we are able to accept
    else:
      self.connections.add(addr)
      self.log("accepted "+addr)
    # Read data from connection
    try:
      while True:
        data = await reader.read(32224)                                 # 1024*16 bytes
        if not data:
          break
        self.producer.send(self.topic,data)
    except asyncio.CancelledError:
      pass
    finally:
      self.connections.remove(addr)
      self.log("closed "+addr)
      writer.close()

  def cleanup(self):
    self.log("shutdown")
    self.producer.flush()
    self.producer.close()

def main():
  options = docopt(__doc__)
  kafka_host = options["<kafkaHost>"]
  kafka_port = options["<kafkaPort>"]
  tcp_host = options["<tcpHost>"]
  tcp_port = options["<tcpPort>"]
  topic = options["<topic>"]
  log_topic = options["<logTopic>"]
  tcp_server_producer = TCPServerProducer(kafka_host,kafka_port,
                                                tcp_host,tcp_port,
                                                topic,log_topic)
  try:
    tcp_server_producer.run()
  except KeyboardInterrupt:
    pass
  finally:
    tcp_server_producer.cleanup()
  
if __name__ == "__main__":
  main()
