from distutils.core import setup

setup(
  name='kafka-tcp-server-producer',
  version='0.1.0',
  author='Lingyu Zhou',
  author_email='zhoulytwin@gmail.com',
  scripts=['bin/kafka-tcp-server-producer.py'],
  url='https://github.com/TwistTRL/kafka-tcp-server-producer',
  license='GPL-3.0',
  description='https://github.com/TwistTRL/kafka-tcp-server-producer',
  install_requires=[
    "docopt >= 0.6.1",
    "kafka-python >= 1.4.6"
  ],
)
