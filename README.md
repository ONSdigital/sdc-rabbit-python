# sdc-rabbit

[![Build Status](https://travis-ci.org/ONSdigital/sdc-rabbit.svg?branch=master)](https://travis-ci.org/ONSdigital/sdc-rabbit)
[![codecov](https://codecov.io/gh/ONSdigital/sdc-rabbit/branch/master/graph/badge.svg)](https://codecov.io/gh/ONSdigital/sdc-rabbit)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/043810e79dac47759cc661361a8af12b)](https://www.codacy.com/app/ONS/sdc-rabbit?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ONSdigital/sdc-rabbit&amp;utm_campaign=Badge_Grade)

Common code for SDC Pika-based services that interact with RabbitMQ.

## sdc-rabbit

A common source code library for SDC apps that use Pika to interact with RabbitMQ.
To install, use `pip install sdc-rabbit`.

### Basic Use

Assuming you are executing from inside an activated virtual environment:

### Install requirements:

```
$ make install
```

### Run the unit tests:

In order to run the tests you will need a copy of rabbitmq running.  The easiest way of doing this is with docker with the following command:

```
docker run -d -p 5672:5672 rabbitmq:3-management
```

then to run the tests, just do:

```
$ make test
```

#### Create a package for deployment:

```
$ make dist
```

#### Test package locally

Inside of a virtual environment, this can be installed from git with the following command:
```
pip install git+git://github.com/ONSDigital/sdc-rabbit.git@<branch-name-here>#egg=sdc-rabbit
```


## PyPi

This repo is available in PyPi at [sdc-rabbit](https://pypi.python.org/pypi/sdc-rabbit)

The package is published automatically to PyPi when a tag is created in Github. The configuration for this is in the
[.travis.yml](.travis.yml) file.
