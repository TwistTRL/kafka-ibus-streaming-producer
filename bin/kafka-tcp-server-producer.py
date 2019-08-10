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

class IBUSTCPServerProducer:
  MAX_CONNECTION = 1
  LOG_FORMAT ="{} UTC_TS\t"\
              "{}"
              
  def __init__(self,kafkaHost,kafkaPort,
                    tcpHost,tcpPort,
                    topic,logTopic):
    self.kafkaHost = kafkaHost
    self.kafkaPort = kafkaPort
    self.tcpHost = tcpHost
    self.tcpPort = tcpPort
    self.topic = topic
    self.logTopic = logTopic
    self.producer = KafkaProducer(bootstrap_servers=["{}:{}".format(kafkaHost,kafkaPort)])
    self.connections = set()
    
  def log(self,msg):
    self.producer.send( self.logTopic,
                        self.LOG_FORMAT.format( datetime.now().timestamp(),
                                                msg
                                                ) \
                            .encode()
                        )

  def run(self):
    self.log("running")
    asyncio.run(self._async_run())
    
  async def _async_run(self):
    tcpServer = await asyncio.start_server(self.connection_handler,self.tcpHost,self.tcpPort)
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
  kafkaHost = options["<kafkaHost>"]
  kafkaPort = options["<kafkaPort>"]
  tcpHost = options["<tcpHost>"]
  tcpPort = options["<tcpPort>"]
  topic = options["<topic>"]
  logTopic = options["<logTopic>"]
  IBUSTCPServerProducer = IBUSTCPServerProducer(kafkaHost,kafkaPort,
                                                tcpHost,tcpPort,
                                                topic,logTopic)
  try:
    IBUSTCPServerProducer.run()
  except KeyboardInterrupt:
    pass
  finally:
    IBUSTCPServerProducer.cleanup()
  
if __name__ == "__main__":
  main()