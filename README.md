# SddF - Scalable Duplicate Detection Framework utilizing Apache
SddF is supposed to be a scalable distributed duplicate detection framework.
Apache Spark is used as distributed computing framework.
SddF is the result of my master's thesis in computational science.
It is still a prototype and not supposed to be used in production.
Detailed information can be found in my [thesis](scalable-duplicate-detection.pdf).

# Getting started
There is no binary release of SddF at the moment.
Therefor you have to build it on your own.
The following lines are briefly describing how to get started.

## Prerequisits
The Scala Build Tool (SBT) needs to be installed.
Look at the SBT docs to find the installation procedure for your OS.

## Build SddF
* Clone the sddf repository
* Run sbt publishLocal to compile SddF and make it available to other projects locally.
```sh
git clone git@github.com:numbnut/sddf.git
sbt publishLocal
```

## Run Example
* Clone the sddf-example repository
* Run the example class
```sh
git clone git@github.com:numbnut/sddf-example.git
sbt run
```

# License
SddF is licensed under the GPLv3.
