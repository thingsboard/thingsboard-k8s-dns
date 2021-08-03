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
import java.util.List;

@Slf4j
public class DNSMessageHandler extends SimpleChannelInboundHandler<DatagramDnsQuery> {

    private final K8sResolverService resolverService;

    public DNSMessageHandler(K8sResolverService resolverService) {
        this.resolverService = resolverService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramDnsQuery query) throws Exception {
        log.trace("Processing DNS query: {}", query);
        DatagramDnsResponse response = new DatagramDnsResponse(query.recipient(), query.sender(), query.id());
        int count = query.count(DnsSection.QUESTION);
        for (int index = 0; index < count; index ++) {
            DefaultDnsQuestion question = query.recordAt(DnsSection.QUESTION, index);
            response.addRecord(DnsSection.QUESTION, question);
            if (question.type() == DnsRecordType.A) {
                processARecordQuestion(response, question);
            }
        }
        if (response.count(DnsSection.ANSWER) == 0) {
            response.setCode(DnsResponseCode.NXDOMAIN);
        }
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Unexpected Exception", cause);
        ctx.close();
    }

    private void processARecordQuestion(DatagramDnsResponse response, DefaultDnsQuestion question) {
        String endpointName = toEndpointName(question.name());
        List<String> addressList = resolverService.resolveEndpoint(endpointName);
        addressList.forEach(strAddress -> {
            try {
                byte[] address = SocketUtils.addressByName(strAddress).getAddress();
                DefaultDnsRawRecord queryAnswer = new DefaultDnsRawRecord(question.name(),
                        DnsRecordType.A, 3600, Unpooled.wrappedBuffer(address));
                response.addRecord(DnsSection.ANSWER, queryAnswer);
            } catch (UnknownHostException e) {
                log.error("Failed to resolve address: {}", strAddress, e);
            }
        });
    }

    private String toEndpointName(String domainName) {
        if (domainName.endsWith(".")) {
            return domainName.substring(0, domainName.length()-1);
        }
        return domainName;
    }
}
