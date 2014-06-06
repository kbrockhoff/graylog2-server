/**
 * Copyright 2014 Lennart Koopmann <lennart@torch.sh>
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.graylog2.inputs.gelf.amqp;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.graylog2.inputs.gelf.gelf.GELFMessage;
import org.graylog2.inputs.gelf.gelf.GELFProcessor;
import org.graylog2.plugin.InputHost;
import org.graylog2.plugin.buffers.BufferOutOfCapacityException;
import org.graylog2.plugin.configuration.ConfigurationException;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * AMQP input that behaves like the pre 0.20.x AMQP input.
 *
 * @author Kevin Brockhoff <kbrockhoff@codekaizen.org>
 */
public class GELFAMQPInput extends MessageInput {

    public static final String CK_HOSTNAME = "broker_hostname";
    public static final String CK_VHOST = "broker_vhost";
    public static final String CK_USERNAME = "broker_username";
    public static final String CK_PASSWORD = "broker_password";
    public static final String CK_PREFETCH = "prefetch";
    public static final String CK_QUEUE = "queue";
    public static final String CK_ROUTING_KEY = "routing_key";
    public static final String NAME = "GELF AMQP";
    public static final int WS_CONN_TIMEOUT = 3000;

    private static final Logger LOG = LoggerFactory.getLogger(GELFAMQPInput.class);
    private static final String DEFAULT_ROUTEKEY = "#";

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final Semaphore initializeSemaphore = new Semaphore(1);
    private Address[] addresses = new Address[0];
    private String username = ConnectionFactory.DEFAULT_USER;
    private String password = ConnectionFactory.DEFAULT_PASS;
    private String virtualHost = ConnectionFactory.DEFAULT_VHOST;
    private int connectionTimeout = WS_CONN_TIMEOUT;
    private final AtomicReference<Executor> executorService = new AtomicReference<Executor>();
    private boolean createdExecutorService = false;
    private ConnectionFactory connectionFactory;
    private final AtomicReference<Connection> connection = new AtomicReference<Connection>();
    private final AtomicBoolean shutdownSignalReceived = new AtomicBoolean(false);
    private String queueName;
    private final AtomicBoolean acknowledgingReceipt = new AtomicBoolean(false);
    private String routingKey = DEFAULT_ROUTEKEY;
    private MessageListener messageListener;
    private final AtomicReference<String> consumerTag = new AtomicReference<String>();

    public boolean isInitialized()
    {
        return initialized.get();
    }

    public void initialize() throws MisfireException
    {
    }

    public boolean isTerminated()
    {
        return terminated.get();
    }

    @Override
    public void launch() throws MisfireException {
        blockOperationsIfNotAlreadyBlocked();
        try
        {
            initialized.set(false);
            doConfiguration();
            doInitialization();
            initialized.set(true);
            LOG.info("{} subcomponent initialized", getName());
        }
        catch (final RuntimeException exception)
        {
            throw new MisfireException("unable to start " + NAME, exception);
        }
        finally
        {
            unblockOperationsIfBlocked();
        }
    }

    @Override
    public void stop() {
        if (isInitialized() && !isTerminated()) {
            initializeSemaphore.acquireUninterruptibly();
            try {
                doTermination();
                terminated.set(true);
                LOG.info("{} subcomponent terminated", getName());
            } finally {
                initializeSemaphore.release();
            }
        }
    }

    protected void doConfiguration() {
        final String addrStr = configuration.getString(CK_HOSTNAME);
        LOG.info("setting addresses={}", addrStr);
        parseServerAddresses(addrStr);
        final String user = configuration.getString(CK_USERNAME);
        LOG.info("setting username={}", user);
        setUsername(user);
        final String passwd = configuration.getString(CK_PASSWORD);
        LOG.info("setting password={}", passwd);
        setPassword(passwd);
        final String vHost = configuration.getString(CK_VHOST);
        LOG.info("setting virtualHost={}", vHost);
        setVirtualHost(vHost);
        final String queue = configuration.getString(CK_QUEUE);
        LOG.info("setting queueName={}", queue);
        setQueueName(queue);
        final String route = configuration.getString(CK_ROUTING_KEY);
        LOG.info("setting routingKey={}", route);
        setRoutingKey(route);
    }

    protected void doInitialization() {
        constructExecutorServiceIfNeeded();
        constructConnectionFactoryIfNeeded();
        getConnection();
        doChannelInitialization();
    }
    
    protected void doTermination() {
        doChannelTermination();
        closeConnectionQuietly();

        if (createdExecutorService && getExecutorService() instanceof ExecutorService) {
            final ExecutorService svc = (ExecutorService) getExecutorService();
            if (!svc.isShutdown()) {
                svc.shutdownNow();
            }
        }
    }
    
    protected void doChannelInitialization() {
        if (messageListener != null) {
            messageListener.terminate();
            messageListener = null;
        }
        messageListener = new MessageListener(graylogServer, this);
        messageListener.initialize();
        LOG.info("starting messageListener...");
        getExecutorService().execute(messageListener);
    }

    protected void doChannelTermination() {
        if (messageListener != null) {
            messageListener.terminate();
            messageListener = null;
        }
    }

    @Override
    public ConfigurationRequest getRequestedConfiguration() {
        ConfigurationRequest cr = new ConfigurationRequest();

        cr.addField(
                new TextField(
                        CK_HOSTNAME,
                        "Broker hostname",
                        "",
                        "Comma-separated hostname:port list of the AMQP brokers to use",
                        ConfigurationField.Optional.NOT_OPTIONAL
                )
        );

        cr.addField(
                new TextField(
                        CK_VHOST,
                        "Broker virtual host",
                        "/",
                        "Virtual host of the AMQP broker to use",
                        ConfigurationField.Optional.NOT_OPTIONAL
                )
        );

        cr.addField(
                new TextField(
                        CK_USERNAME,
                        "Username",
                        "",
                        "Username to connect to AMQP broker",
                        ConfigurationField.Optional.OPTIONAL
                )
        );

        cr.addField(
                new TextField(
                        CK_PASSWORD,
                        "Password",
                        "",
                        "Password to connect to AMQP broker",
                        ConfigurationField.Optional.OPTIONAL,
                        TextField.Attribute.IS_PASSWORD
                )
        );

        cr.addField(
                new NumberField(
                        CK_PREFETCH,
                        "Prefetch count",
                        0,
                        "For advanced usage: AMQP prefetch count. Default is 0 (unlimited).",
                        ConfigurationField.Optional.NOT_OPTIONAL
                )
        );

        cr.addField(
                new TextField(
                        CK_QUEUE,
                        "Queue",
                        "log-messages",
                        "Name of queue that is created.",
                        ConfigurationField.Optional.NOT_OPTIONAL
                )
        );

        cr.addField(
                new TextField(
                        CK_ROUTING_KEY,
                        "Routing key",
                        "#",
                        "Routing key to listen for.",
                        ConfigurationField.Optional.NOT_OPTIONAL
                )
        );

        return cr;
    }

    @Override
    public void checkConfiguration() throws ConfigurationException
    {
        if (!configuration.stringIsSet(CK_HOSTNAME)
                || !configuration.stringIsSet(CK_QUEUE)) {
            throw new ConfigurationException(configuration.getSource().toString());
        }
    }

    @Override
    public Map<String, Object> getAttributes()
    {
        return configuration.getSource();
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String linkToDocs() {
        return "";
    }

    public Executor getExecutorService() {
        return executorService.get();
    }

    public void setExecutorService(final Executor executorService) {
        if (executorService != null) {
            this.executorService.set(executorService);
        }
    }
    
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(final ConnectionFactory connectionFactory) {
        if (connectionFactory == null) {
            throw new IllegalArgumentException("connectionFactory cannot be null");
        }
        this.connectionFactory = connectionFactory;
    }
    
    protected Address[] getAddresses() {
        return addresses;
    }

    protected void setAddresses(final Address[] addresses) {
        if (addresses == null || addresses.length == 0) {
            throw new IllegalArgumentException("addresses cannot be empty");
        }
        this.addresses = addresses;
    }

    protected String getUsername() {
        return username;
    }

    protected void setUsername(final String username) {
        this.username = username;
    }

    protected String getPassword() {
        return password;
    }

    protected void setPassword(final String password) {
        this.password = password;
    }

    protected String getVirtualHost() {
        return virtualHost;
    }

    protected void setVirtualHost(final String virtualHost) {
        if (StringUtils.isBlank(virtualHost)) {
            this.virtualHost = ConnectionFactory.DEFAULT_VHOST;
        } else {
            this.virtualHost = virtualHost;
        }
    }

    protected int getConnectionTimeout() {
        return connectionTimeout;
    }

    protected void setConnectionTimeout(final int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    protected String getQueueName() {
        return queueName;
    }

    protected void setQueueName(final String queueName) {
        if (StringUtils.isBlank(queueName) && StringUtils.isBlank(this.queueName)) {
            throw new IllegalArgumentException("queueName cannot be blank");
        }
        if (!StringUtils.isBlank(queueName)) {
            this.queueName = queueName;
        }
    }

    protected String getRoutingKey() {
        return routingKey;
    }

    protected void setRoutingKey(final String routingKey) {
        if (StringUtils.isNotBlank(routingKey)) {
            this.routingKey = routingKey;
        }
    }

    protected boolean isAcknowledgingReceipt() {
        return acknowledgingReceipt.get();
    }

    protected void setAcknowledgingReceipt(final boolean acknowledgingReceipt) {
        this.acknowledgingReceipt.set(acknowledgingReceipt);
    }

    protected String getPrimaryServerHost() {
        return getAddresses()[0].getHost();
    }

    protected int getPrimaryServerPort() {
        return getAddresses()[0].getPort();
    }

    protected Connection getConnection() {
        if (connection.get() == null || !connection.get().isOpen()) {
            closeConnectionQuietly();
            connection.set(createConnection());
        }
        return connection.get();
    }

    protected void checkConnectionStatusAndReconnectIfNeeded() {
        final boolean needToUnblock = blockOperationsIfNotAlreadyBlocked();
        try {
            if (isInitialized() && !isTerminated() && connection.get() != null
                    && (!connection.get().isOpen() || shutdownSignalReceived.get())) {
                LOG.warn("attempting to reestablish connection to the server");
                closeConnectionAndThenReconnect();
                shutdownSignalReceived.set(false);
            }
        } finally {
            if (needToUnblock) {
                unblockOperationsIfBlocked();
            }
        }
    }

    protected boolean blockOperationsIfNotAlreadyBlocked() {
        final boolean result = initializeSemaphore.availablePermits() > 0;
        if (result) {
            initializeSemaphore.acquireUninterruptibly();
        }
        return result;
    }
    
    protected void unblockOperationsIfBlocked() {
        if (initializeSemaphore.availablePermits() == 0) {
            initializeSemaphore.release();
        }
    }
    
    private void closeConnectionAndThenReconnect() {
        LOG.trace("closeConnectionAndThenReconnect()");
        doChannelTermination();
        closeConnectionQuietly();
        getConnection();
        doChannelInitialization();
    }

    private final Connection createConnection() {
        LOG.trace("createConnection()");
        Connection connection = null;
        IllegalStateException mfException = null;
        for (int i = 0; i < 3; i++) {
            LOG.info("attempt {} to create connection", i);
            try {
                connection = getConnectionFactory().newConnection(getAddresses());
            } catch (final Exception cause) {
                LOG.info("connection attempt {} failed: {}", cause.getMessage());
                connection = null;
                mfException = new IllegalStateException("unable to connect to AMQP server", cause);
            }
            if (connection == null) {
                try {
                    Thread.sleep(5000L);
                } catch (final InterruptedException ignore) {
                    // NoOp
                }
            } else {
                break;
            }
        }
        if (connection == null && mfException != null) {
            throw mfException;
        }
        return connection;
    }

    private void closeConnectionQuietly() {
        LOG.trace("closeConnectionQuietly()");
        if (connection.get() != null && connection.get().isOpen()) {
            LOG.info("closing connection");
            try {
                connection.get().close(getConnectionTimeout());
            } catch (final Exception ignore) {
                // do nothing
            }
        }
        connection.set(null);
    }

    private void constructExecutorServiceIfNeeded() {
        if (getExecutorService() == null) {
            LOG.info("constructing executor service");
            final ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
            builder.setNameFormat("amqp-client-pool-%d");
            setExecutorService(Executors.newCachedThreadPool(builder.build()));
            createdExecutorService = true;
        }
    }

    private void constructConnectionFactoryIfNeeded() {
        if (getConnectionFactory() == null) {
            LOG.info("constructing AMQP connection factory");
            final ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(getPrimaryServerHost());
            factory.setPort(getPrimaryServerPort());
            factory.setUsername(getUsername());
            factory.setPassword(getPassword());
            factory.setVirtualHost(getVirtualHost());
            factory.setConnectionTimeout(getConnectionTimeout());
            setConnectionFactory(factory);
        }
    }

    private void parseServerAddresses(final String addrStr)
    {
        Address[] addressArray = null;
        if (StringUtils.isNotBlank(addrStr)) {
            addressArray = Address.parseAddresses(addrStr);
        }
        if (addressArray == null || addressArray.length == 0) {
            LOG.info("no server addresses supplied so using default");
            addressArray = new Address[1];
            addressArray[0] = new Address(getDefaultHostName(), ConnectionFactory.DEFAULT_AMQP_PORT);
        }
        setAddresses(addressArray);
    }

    private String getDefaultHostName() {
        String result;
        try {
            final InetAddress localMachine = InetAddress.getLocalHost();
            result = localMachine.getHostName();
            LOG.debug("Using hostname [{}] for hostname.", result);
        } catch (final UnknownHostException cause) {
            LOG.warn("Could not get host name, using 'localhost' as default value", cause);
            result = "localhost";
        }
        return result;
    }

    private String getConsumerTag() {
        return consumerTag.get();
    }

    private void setConsumerTag(final String consumerTag) {
        this.consumerTag.set(consumerTag);
    }

    class MessageListener implements Consumer, Runnable {

        private final GELFProcessor gelfProcessor;
        private final MessageInput sourceInput;
        private final Meter receivedMessages;
        private final Meter gelfMessages;
        private Channel channel;
        private final AtomicBoolean terminating = new AtomicBoolean(false);

        MessageListener(final InputHost server, final MessageInput sourceInput) {
            super();
            this.gelfProcessor = new GELFProcessor(server);
            this.sourceInput = sourceInput;
            this.receivedMessages = server.metrics().meter(name(GELFAMQPInput.class, "receivedMessages"));
            this.gelfMessages = server.metrics().meter(name(GELFAMQPInput.class, "gelfMessages"));
        }

        void initialize() {
            LOG.info("creating subscriber channel");
            try {
                channel = getConnection().createChannel();
                if (!doesQueueExist(channel, getQueueName())) {
                    getChannel().queueDeclare(getQueueName(), true, false, false, null);
                }
                final String consumerTag = getChannel().basicConsume(getQueueName(), !isAcknowledgingReceipt(), this);
                setConsumerTag(consumerTag);
            } catch (final Exception cause) {
                throw new IllegalStateException("unable to create channel", cause);
            }
        }

        void terminate() {
            terminating.set(true);
            if (getConsumerTag() != null) {
                try {
                    getChannel().basicCancel(getConsumerTag());
                } catch (final Exception ignore) {
                    LOG.info("exception cancelling consumer: {}", ignore.getMessage());
                }
                setConsumerTag(null);
            }
        }

        @Override
        public void run() {
            LOG.info("Starting up AMQP message consumer loop");
            while (!isTerminated()) {
                try {
                    LOG.trace("sleeping for {} ms", 250L);
                    Thread.sleep(250L);
                } catch (final InterruptedException e) {
                    LOG.debug("AMQP handler loop interrupted: {}", e.getMessage());
                }
            }
            LOG.info("Shutting down AMQP message consumer loop");
        }

        @Override
        public void handleConsumeOk(final String consumerTag) {
            LOG.info("AMQP consumer registered with tag={}", consumerTag);
        }

        @Override
        public void handleCancelOk(final String consumerTag) {
            LOG.info("AMQP cancel.Ok message received");
        }

        @Override
        public void handleCancel(final String consumerTag) throws IOException {
            LOG.info("AMQP cancel message received");
        }

        @Override
        public void handleDelivery(final String consumerTag, final Envelope envelope, final BasicProperties properties,
                final byte[] body) throws IOException {
            LOG.debug("delivery handled");
            receivedMessages.mark();
            final GELFMessage msg = new GELFMessage(body);
            try {
                gelfProcessor.messageReceived(msg, sourceInput);
                gelfMessages.mark();
            } catch (final BufferOutOfCapacityException cause) {
                throw new IOException(cause);
            }
            if (isAcknowledgingReceipt()) {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
            }
        }

        @Override
        public void handleShutdownSignal(final String consumerTag, final ShutdownSignalException sig) {
            LOG.warn("AMQP shutdown signal received");
            if (!terminating.get()) {
                checkConnectionStatusAndReconnectIfNeeded();
            }
        }

        @Override
        public void handleRecoverOk(final String consumerTag) {
            LOG.info("AMQP recover.Ok message received");
        }

        Channel getChannel() {
            return channel;
        }

        boolean doesQueueExist(final Channel channel, final String queueName) {
            boolean result = true;
            try {
                channel.queueDeclarePassive(queueName);
            } catch (final IOException cause) {
                result = false;
                LOG.info("exception caught on exchange existence check", cause);
            } catch (final Exception ignore) {
                LOG.info("exception caught on exchange existence check", ignore);
            }
            return result;
        }

    }

}
