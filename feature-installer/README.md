# Project Description


---






# How to use the project

## Contact Information
#### PO
abc <a href="mailto:abc@xyz.com"> abc@xyz.com</a>  
#### Team Members  

abc <a href="abc@xyz.com"> abc@xyz.com</a>  


#### Email
Guardians for this project can be reached at <a href="abc@xyz.com">abc@xyzcom</a>  


## Maven Dependencies
The chassis has the following Maven dependencies:
  - Spring Boot Start Parent version 2.1.5.
  - Spring Boot Starter Web.
  - Spring Boot Actuator.
  - Spring Cloud Sleuth.
  - Spring Boot Started Test.
  - JaCoCo Code Coverage Plugin.
  - Sonar Maven Plugin.
  - Spotify Dockerfile Maven Plugin.
  - Common Logging utility for logback created by Vortex team. [EO Common Logging]
  - Properties for spring cloud version and java are as follows.
  ```
      <java.version>8</java.version>
      <spring-cloud.version>Greenwich.SR1</spring-cloud.version>
  ```

## Build related artifacts
The main build tool is BOB provided by ADP. For convenience, maven wrapper is provided to allow the developer to build in an isolated workstation that does not have access to ADP.
  - [ruleset2.0.yaml](ruleset2.0.yaml) - for more details on BOB please click here [BOB].



## Containerization and Deployment to Kubernetes cluster.
Following artifacts contains information related to building a container and enabling deployment to a Kubernetes cluster:
- [charts](charts/) folder - used by BOB to lint, package and upload helm chart to helm repository.
  -  
- [Dockerfile](Dockerfile) - used by Spotify dockerfile maven plugin to build docker image.
  - The base image for the chassis application is ```sles-jdk8``` available in ```armdocker.rnd.ericsson.se```.

## Source
The [src](src/) folder of the java project contains a core spring boot application, a controller for health check and an interceptor for helping with logging details like user name. The folder also contains corresponding java unit tests.

  ```
		Paste the Project Structure here 
  ```

