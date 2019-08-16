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

MESSAGE_START_BYTE = b'\x0B'
MESSAGE_END_BYTE = b'\x1C'+b'\x0D'
MESSAGE_SEGMENT_END_BYTE = b'\x0D'

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
    addr = str(writer.get_extra_info("peername"))
    # A new connection, but we can accept no more
    if addr not in self.connections and \
        len(connections)>=self.MAX_CONNECTION:
      self.refuse_client(addr,writer)
      return
    # Add connection
    self.add_client(addr,writer)
    # Read data from connection
    remaining_data = b""
    try:
      while True:
        data = await reader.read(8192)                                  # 1024*8 bytes
        if not data:
          break
        data = remaining_data + data
        extraction = self.extract_hl7_messages(data)
        messages = extraction["messages"]
        remaining_data = extraction["remaining"]
        for msg in messages:
          self.producer.send(self.topic,msg)
    except BrokenPipeError:
      """
      Catches connecton reset by peer when we are sending the batched data,
       which is also when we cannot check for reader. The broken connection
       on the writer side will ultimately lead to  BrokenPipeError on the
       reader side. Hence
      """
      pass
    finally:
      self.remove_client(addr)
  
  def extract_hl7_messages(self,byte_stream):
    messages = []
    remaining = byte_stream
    while True:
      try:
        start_idx = remaining.index(MESSAGE_START_BYTE)
        end_idx = remaining.index(MESSAGE_END_BYTE,start_idx+1)+1
        msg = remaining[start_idx:end_idx+1]
        messages.append(msg)
        remaining = remaining[end_idx+1:]
      except ValueError:
        break
    return {"messages":messages,"remaining":remaining}

  def refuse_client(self,addr,writer):
    self.log("{} refused".format(addr))
    writer.close()
  
  def add_client(self,addr,writer):
    if addr not in self.connections:
      self.log("{} accepted".format(addr))
      self.connections[addr] = writer
    else:
      self.remove_client(addr)
      self.add_client(addr)
  
  def remove_client(self,addr):
    if addr in self.connections:
      self.log("{} closed".format(addr))
      writer = self.connections.pop(addr)
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
