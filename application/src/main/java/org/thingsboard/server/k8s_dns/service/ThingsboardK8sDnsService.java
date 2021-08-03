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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.dns.DatagramDnsQueryDecoder;
import io.netty.handler.codec.dns.DatagramDnsResponseEncoder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Service("ThingsboardK8sDnsService")
@Slf4j
public class ThingsboardK8sDnsService {

    @Value("${k8s_dns.bind_address}")
    private String host;
    @Value("${k8s_dns.bind_port}")
    private Integer port;
    @Value("${k8s_dns.netty.worker_group_thread_count}")
    private Integer workerGroupThreadCount;

    @Autowired
    private K8sResolverService resolverService;

    private Channel serverChannel;
    private EventLoopGroup workerGroup;

    @PostConstruct
    public void init() throws Exception {
        log.info("Starting ThingsBoard K8S DNS Service...");
        workerGroup = new NioEventLoopGroup(workerGroupThreadCount);
        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel nioDatagramChannel) throws Exception {
                        nioDatagramChannel.pipeline().addLast(new DatagramDnsQueryDecoder());
                        nioDatagramChannel.pipeline().addLast(new DatagramDnsResponseEncoder());
                        nioDatagramChannel.pipeline().addLast(new DNSMessageHandler(resolverService));
                    }
                }).option(ChannelOption.SO_BROADCAST, true);

        serverChannel = b.bind(host, port).sync().channel();

        log.info("ThingsBoard K8S DNS Service started!");
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("Stopping ThingsBoard K8S DNS Service!");
        try {
            serverChannel.close().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
        log.info("ThingsBoard K8S DNS Service stopped!");
    }
}
