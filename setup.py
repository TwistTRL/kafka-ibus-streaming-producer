from distutils.core import setup

setup(
  name='kafka-ibus-streaming-producer',
  version='0.1.0',
  author='Lingyu Zhou',
  author_email='zhoulytwin@gmail.com',
  scripts=['bin/kafka-ibus-streaming-producer.py'],
  url='https://github.com/TwistTRL/kafka-ibus-streaming-producer',
  license='GPL-3.0',
  description='https://github.com/TwistTRL/kafka-ibus-streaming-producer',
  install_requires=[
    "docopt >= 0.6.1",
    "kafka-python >= 1.4.6"
  ],
)
