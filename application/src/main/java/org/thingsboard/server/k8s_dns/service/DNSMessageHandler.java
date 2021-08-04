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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.dns.DatagramDnsQuery;
import io.netty.handler.codec.dns.DatagramDnsResponse;
import io.netty.handler.codec.dns.DefaultDnsQuestion;
import io.netty.handler.codec.dns.DefaultDnsRawRecord;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsResponseCode;
import io.netty.handler.codec.dns.DnsSection;
import io.netty.util.internal.SocketUtils;
import lombok.extern.slf4j.Slf4j;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DNSMessageHandler extends SimpleChannelInboundHandler<DatagramDnsQuery> {

    private final K8sResolverService resolverService;

    private final LoadingCache<String, List<byte[]>> resolverCache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(10, TimeUnit.SECONDS).build(
                    new CacheLoader<>() {
                        public List<byte[]> load(String key) {
                            return resolveDomain(key);
                        }
                    });

    public DNSMessageHandler(K8sResolverService resolverService) {
        this.resolverService = resolverService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramDnsQuery query) throws Exception {
        log.trace("Processing DNS query: {}", query);
        DatagramDnsResponse response = new DatagramDnsResponse(query.recipient(), query.sender(), query.id());
        boolean resolved = false;
        int count = query.count(DnsSection.QUESTION);
        for (int index = 0; index < count; index ++) {
            DefaultDnsQuestion question = query.recordAt(DnsSection.QUESTION, index);
            response.addRecord(DnsSection.QUESTION, question);
            if (question.type() == DnsRecordType.A || question.type() == DnsRecordType.AAAA) {
                List<byte[]> addresses = resolverCache.get(question.name());
                if (question.type() == DnsRecordType.A) {
                    addresses.forEach(address -> {
                        DefaultDnsRawRecord queryAnswer = new DefaultDnsRawRecord(question.name(),
                                DnsRecordType.A, 3600, Unpooled.wrappedBuffer(address));
                        response.addRecord(DnsSection.ANSWER, queryAnswer);
                    });
                }
                if (!addresses.isEmpty()) {
                    resolved = true;
                }
            }
        }
        if (!resolved) {
            response.setCode(DnsResponseCode.NXDOMAIN);
        }
        log.trace("DNS query response: {}", response);
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Unexpected Exception", cause);
        ctx.close();
    }

    private List<byte[]> resolveDomain(String domainName) {
        String endpointName = toEndpointName(domainName);
        List<String> addressList = resolverService.resolveEndpoint(endpointName);
        List<byte[]> result = new ArrayList<>();
        addressList.forEach(strAddress -> {
            try {
                byte[] address = SocketUtils.addressByName(strAddress).getAddress();
                result.add(address);
            } catch (UnknownHostException e) {
                log.error("Failed to resolve address: {}", strAddress, e);
            }
        });
        return result;
    }

    private String toEndpointName(String domainName) {
        if (domainName.endsWith(".")) {
            return domainName.substring(0, domainName.length()-1);
        }
        return domainName;
    }
}
