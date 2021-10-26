# ThingsBoard k8s DNS server

The DNS server that is working inside the K8S cluster and allows external services to get IPs of internal services exposed through K8S NodePorts.
We have created this project to connect external UDP load balancer with the CoAP/LwM2M Transports, because existing K8S LoadBalancer implementations did not support our use case.
