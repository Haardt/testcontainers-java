package org.testcontainers.dockerclient;

import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NamedPipeSocketClientProviderStrategy extends DockerClientProviderStrategy {

    private static final String NAMED_PIPE_FILE_NAME = "\\\\.\\pipe\\docker_engine";
    private static final String PING_TIMEOUT_DEFAULT = "10";
    private static final String PING_TIMEOUT_PROPERTY_NAME = "testcontainers.namedpipesocketprovider.timeout";

    @Override
    public void test() throws InvalidConfigurationException {
        File file = new File(NAMED_PIPE_FILE_NAME);

        if (!file.exists()) {
            throw new InvalidConfigurationException("this strategy only works with named pipe file");
        }

        NamedPipeProxy proxy = new NamedPipeProxy(file);

        try {
            int proxyPort = proxy.start().getPort();

            config = tryConfiguration("tcp://localhost:" + proxyPort);

            LOGGER.info("Accessing named pipe socket via TCP proxy (" + file + " via localhost:" + proxyPort + ")");
        } catch (Exception e) {
            proxy.stop();

            throw new InvalidConfigurationException("ping failed", e);
        } finally {
            Runtime.getRuntime().addShutdownHook(new Thread(proxy::stop));
        }
    }

    @Override
    public String getDescription() {
        return "Named pipe socket \"" + NAMED_PIPE_FILE_NAME + "\" (via TCP proxy)";
    }

    @NotNull
    protected DockerClientConfig tryConfiguration(String dockerHost) {
        config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .withDockerHost(dockerHost)
                .withDockerTlsVerify(false)
                .build();
        client = getClientForConfig(config);

        final int timeout = Integer.parseInt(System.getProperty(PING_TIMEOUT_PROPERTY_NAME, PING_TIMEOUT_DEFAULT));
        ping(client, timeout);

        return config;
    }

    @Slf4j
    static class NamedPipeProxy {

        final ExecutorService executorService = Executors.newCachedThreadPool();

        RandomAccessFile randomAccessFile;

        @SneakyThrows(FileNotFoundException.class)
        public NamedPipeProxy(File file) {
            randomAccessFile = new RandomAccessFile(file, "rw");
        }

        InetSocketAddress start() throws IOException {
            ServerSocket listenSocket = new ServerSocket();
            listenSocket.bind(new InetSocketAddress("localhost", 0));

            executorService.submit(() -> {
                log.debug("Listening on {} and proxying to {}", listenSocket.getLocalSocketAddress(), randomAccessFile);

                try {
                    while (!Thread.interrupted()) {
                        try {
                            Socket incomingSocket = listenSocket.accept();
                            log.debug("Accepting incoming connection from {}", incomingSocket.getRemoteSocketAddress());

                            executorService.submit(() -> {
                                try (
                                        InputStream in = incomingSocket.getInputStream();
                                        FileOutputStream out = new FileOutputStream(randomAccessFile.getFD())
                                ) {
                                    return IOUtils.copyLarge(in, out);
                                }
                            });
                            executorService.submit(() -> {
                                try (
                                        FileInputStream in = new FileInputStream(randomAccessFile.getFD());
                                        OutputStream out = incomingSocket.getOutputStream()
                                ) {
                                    return IOUtils.copyLarge(in, out);
                                }
                            });

                        } catch (IOException e) {
                            log.warn("", e);
                        }
                    }
                } finally {
                    listenSocket.close();
                }
                return null;
            });
            return (InetSocketAddress) listenSocket.getLocalSocketAddress();
        }

        public void stop() {
            try {
                executorService.shutdownNow();
            } catch (Exception e) {
                log.warn("", e);
            }
        }
    }
}
