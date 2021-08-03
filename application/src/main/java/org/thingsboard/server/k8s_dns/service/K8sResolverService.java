/**
 * Copyright © 2016-2021 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.k8s_dns.service;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1EndpointAddress;
import io.kubernetes.client.openapi.models.V1EndpointSubset;
import io.kubernetes.client.openapi.models.V1Endpoints;
import io.kubernetes.client.util.ClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service("K8sResolverService")
@Slf4j
public class K8sResolverService {

    @Value("${k8s_dns.namespace}")
    private String namespace;

    private CoreV1Api k8sApi;

    @PostConstruct
    public void init() throws Exception {
        log.info("Initializing k8s client...");
        ApiClient client = ClientBuilder.standard(false).build();
        // ApiClient client = ClientBuilder.cluster().build();
        Configuration.setDefaultApiClient(client);
        k8sApi = new CoreV1Api();
        log.info("K8s client initialized.");
    }

    @PreDestroy
    public void destroy() {
    }

    public List<String> resolveEndpoint(String endpointName) {
        try {
            V1Endpoints endpoints = k8sApi.readNamespacedEndpoints(endpointName, namespace, "false", false, false);
            if (endpoints != null) {
                List<V1EndpointSubset> subsets = endpoints.getSubsets();
                if (subsets != null) {
                    List<String> ipList = new ArrayList<>();
                    subsets.forEach(subset -> {
                        List<V1EndpointAddress> addresses = subset.getAddresses();
                        if (addresses != null) {
                            ipList.addAll(addresses.stream().map(V1EndpointAddress::getIp).collect(Collectors.toList()));
                        }
                    });
                    return ipList;
                }
            }
        } catch (ApiException e) {
            log.error("Failed to resolve K8S endpoint [{}], reason: {}", endpointName, e.getMessage());
        }
        return Collections.emptyList();
    }

}
