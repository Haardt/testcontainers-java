package org.testcontainers.dockerclient;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NamedPipeSocketClientProviderStrategy extends UnixSocketClientProviderStrategy {

    @Override
    public void test() throws InvalidConfigurationException {
        File file = new File("\\\\.\\pipe\\docker_engine");

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

    @Slf4j
    static class NamedPipeProxy {

        final ExecutorService executorService = Executors.newCachedThreadPool();

        RandomAccessFile randomAccessFile;

        @SneakyThrows(FileNotFoundException.class)
        public NamedPipeProxy(File file) {
            randomAccessFile = new RandomAccessFile(file, "rw");
        }

        InetSocketAddress start() throws IOException {
            return (InetSocketAddress) executorService.submit(() -> {
                ServerSocket listenSocket = new ServerSocket();
                listenSocket.bind(new InetSocketAddress("localhost", 0));

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
                return listenSocket.getLocalSocketAddress();
            });
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
