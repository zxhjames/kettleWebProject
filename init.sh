#!/bin/bash

mvn install
cd kettle-webapp
mvn clean tomcat7:run

