/**
 * Copyright 2010, 2011, 2012 Lennart Koopmann <lennart@socketfeed.com>
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

package org.graylog2;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.log4j.Level;
import org.graylog2.cluster.Node;
import org.graylog2.cluster.NodeNotFoundException;
import org.graylog2.filters.BlacklistFilter;
import org.graylog2.filters.ExtractorFilter;
import org.graylog2.filters.RewriteFilter;
import org.graylog2.filters.StaticFieldFilter;
import org.graylog2.filters.StreamMatcherFilter;
import org.graylog2.initializers.DroolsInitializer;
import org.graylog2.initializers.PeriodicalsInitializer;
import org.graylog2.inputs.amqp.AMQPInput;
import org.graylog2.inputs.gelf.amqp.GELFAMQPInput;
import org.graylog2.inputs.gelf.http.GELFHttpInput;
import org.graylog2.inputs.gelf.tcp.GELFTCPInput;
import org.graylog2.inputs.gelf.udp.GELFUDPInput;
import org.graylog2.inputs.kafka.KafkaInput;
import org.graylog2.inputs.misc.jsonpath.JsonPathInput;
import org.graylog2.inputs.misc.metrics.LocalMetricsInput;
import org.graylog2.inputs.radio.RadioAMQPInput;
import org.graylog2.inputs.radio.RadioKafkaInput;
import org.graylog2.inputs.random.FakeHttpMessageInput;
import org.graylog2.inputs.raw.tcp.RawTCPInput;
import org.graylog2.inputs.raw.udp.RawUDPInput;
import org.graylog2.inputs.syslog.tcp.SyslogTCPInput;
import org.graylog2.inputs.syslog.udp.SyslogUDPInput;
import org.graylog2.plugin.Tools;
import org.graylog2.plugin.initializers.InitializerConfigurationException;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.lifecycles.Lifecycle;
import org.graylog2.plugins.PluginInstaller;
import org.graylog2.system.activities.Activity;
import org.graylog2.notifications.Notification;
import org.graylog2.outputs.BatchedElasticSearchOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.beust.jcommander.JCommander;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.log4j.InstrumentedAppender;
import com.github.joschi.jadconfig.JadConfig;
import com.github.joschi.jadconfig.RepositoryException;
import com.github.joschi.jadconfig.ValidationException;
import com.github.joschi.jadconfig.repositories.PropertiesRepository;

/**
 * Wrapper for use with Apache Commons Daemon.
 *
 * @author $Author: kbrockhoff $
 * @version $Revision: 201129 $, $Date: 2013-01-03 08:26:44 -0600 (Thu, 03 Jan 2013) $
 */
public class ServerDaemon implements Daemon {

    private static final String[] EMPTY_ARGS = {};
    private static final Logger LOG = LoggerFactory.getLogger(ServerDaemon.class);
    
    private String[] args = EMPTY_ARGS;
    private Core server;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
        
    public ServerDaemon() {
        super();
    }

    @Override
    public void init(final DaemonContext context) throws DaemonInitException {
        args = context.getArguments();
    }

    @Override
    public void start() throws RepositoryException, ValidationException {
        
        // So jung kommen wir nicht mehr zusammen.

        final CommandLineArguments commandLineArguments = new CommandLineArguments();
        final JCommander jCommander = new JCommander(commandLineArguments, args);
        jCommander.setProgramName("graylog2");

        if (commandLineArguments.isShowHelp()) {
            jCommander.usage();
            System.exit(0);
        }

        if (commandLineArguments.isShowVersion()) {
            System.out.println("Graylog2 Server " + Core.GRAYLOG2_VERSION);
            System.out.println("JRE: " + Tools.getSystemInformation());
            System.exit(0);
        }
        
        String configFile = commandLineArguments.getConfigFile();
        LOG.info("Using config file: {}", configFile);

        final Configuration configuration = new Configuration();
        JadConfig jadConfig = new JadConfig(new PropertiesRepository(configFile), configuration);

        LOG.info("Loading configuration");
        try {
            jadConfig.process();
        } catch (RepositoryException e) {
            LOG.error("Couldn't load configuration file: [{}]", configFile, e);
            System.exit(1);
        } catch (ValidationException e) {
            LOG.error("Invalid configuration", e);
            System.exit(1);
        }

        if (configuration.getPasswordSecret().isEmpty()) {
            LOG.error("No password secret set. Please define password_secret in your graylog2.conf.");
            System.exit(1);
        }
        
        if (commandLineArguments.isInstallPlugin()) {
            System.out.println("Plugin installation requested.");
            PluginInstaller installer = new PluginInstaller(
                    commandLineArguments.getPluginShortname(),
                    commandLineArguments.getPluginVersion(),
                    commandLineArguments.isForcePlugin()
            );
            
            installer.install();
            System.exit(0);
        }

        // Are we in debug mode?
        Level logLevel = Level.INFO;
        if (commandLineArguments.isDebug()) {
            LOG.info("Running in Debug mode");
            logLevel = Level.DEBUG;
        }

        // This is holding all our metrics.
        final MetricRegistry metrics = new MetricRegistry();

        // Report metrics via JMX.
        final JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
        reporter.start();

        InstrumentedAppender logMetrics = new InstrumentedAppender(metrics);
        logMetrics.activateOptions();
        org.apache.log4j.Logger.getRootLogger().setLevel(logLevel);
        org.apache.log4j.Logger.getLogger(Main.class.getPackage().getName()).setLevel(logLevel);
        org.apache.log4j.Logger.getRootLogger().addAppender(logMetrics);

        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        LOG.info("Graylog2 {} starting up. (JRE: {})", Core.GRAYLOG2_VERSION, Tools.getSystemInformation());

        // If we only want to check our configuration, we just initialize the rules engine to check if the rules compile
        if (commandLineArguments.isConfigTest()) {
            Core server = new Core();
            server.setConfiguration(configuration);
            DroolsInitializer drools = new DroolsInitializer();
            try {
                drools.initialize(server, null);
            } catch (InitializerConfigurationException e) {
                LOG.error("Drools initialization failed.", e);
            }
            // rules have been checked, exit gracefully
            System.exit(0);
        }

        // Le server object. This is where all the magic happens.
        Core server = new Core();
        server.setLifecycle(Lifecycle.STARTING);

        server.initialize(configuration, metrics);
        
        // Register this node.
        Node.registerServer(server, configuration.isMaster(), configuration.getRestTransportUri());

        Node thisNode = null;
        try {
            thisNode = Node.thisNode(server);
        } catch (NodeNotFoundException e) {
            throw new RuntimeException("Did not find own node. This should never happen.", e);
        }
        if (configuration.isMaster() && !thisNode.isOnlyMaster()) {
            LOG.warn("Detected another master in the cluster. Retrying in {} seconds to make sure it is not "
                    + "an old stale instance.", Node.PING_TIMEOUT);
            try {
                Thread.sleep(Node.PING_TIMEOUT*1000);
            } catch (InterruptedException e) { /* nope */ }
            
            if (!thisNode.isOnlyMaster()) {
                // All devils here.
                String what = "Detected other master node in the cluster! Starting as non-master! "
                        + "This is a mis-configuration you should fix.";
                LOG.warn(what);
                server.getActivityWriter().write(new Activity(what, Main.class));

                // Write a notification.
                Notification.buildNow(server)
                        .addType(Notification.Type.MULTI_MASTER)
                        .addSeverity(Notification.Severity.URGENT)
                        .publishIfFirst();

                configuration.setIsMaster(false);
            } else {
                LOG.warn("Stale master has gone. Starting as master.");
            }
        }
        
        // Enable local mode?
        if (commandLineArguments.isLocal() || commandLineArguments.isDebug()) {
            // In local mode, systemstats are sent to localhost for example.
            LOG.info("Running in local mode");
            server.setLocalMode(true);
        }

        // Are we in stats mode?
        if (commandLineArguments.isStats()) {
            LOG.info("Printing system utilization information.");
            server.setStatsMode(true);
        }


        if (!commandLineArguments.performRetention()) {
            configuration.setPerformRetention(false);
        }

        // propagate default size to input plugins
        MessageInput.setDefaultRecvBufferSize(configuration.getUdpRecvBufferSizes());

        // Register standard inputs.
        server.inputs().register(SyslogUDPInput.class, SyslogUDPInput.NAME);
        server.inputs().register(SyslogTCPInput.class, SyslogTCPInput.NAME);
        server.inputs().register(RawUDPInput.class, RawUDPInput.NAME);
        server.inputs().register(RawTCPInput.class, RawTCPInput.NAME);
        server.inputs().register(GELFUDPInput.class, GELFUDPInput.NAME);
        server.inputs().register(GELFTCPInput.class, GELFTCPInput.NAME);
        server.inputs().register(GELFHttpInput.class, GELFHttpInput.NAME);
        server.inputs().register(FakeHttpMessageInput.class, FakeHttpMessageInput.NAME);
        server.inputs().register(LocalMetricsInput.class, LocalMetricsInput.NAME);
        server.inputs().register(JsonPathInput.class, JsonPathInput.NAME);
        server.inputs().register(KafkaInput.class, KafkaInput.NAME);
        server.inputs().register(RadioKafkaInput.class, RadioKafkaInput.NAME);
        server.inputs().register(AMQPInput.class, AMQPInput.NAME);
        server.inputs().register(RadioAMQPInput.class, RadioAMQPInput.NAME);
        server.inputs().register(GELFAMQPInput.class, GELFAMQPInput.NAME);

        // Register initializers.
        server.initializers().register(new DroolsInitializer());
        server.initializers().register(new PeriodicalsInitializer());

        // Register message filters. (Order is important here)
        server.registerFilter(new StaticFieldFilter());
        server.registerFilter(new ExtractorFilter());
        server.registerFilter(new BlacklistFilter());
        server.registerFilter(new StreamMatcherFilter());
        server.registerFilter(new RewriteFilter());

        // Register outputs.
        server.outputs().register(new BatchedElasticSearchOutput(server));

        
        // initialize the components
        server.initializeComponents();
        
        // Set running
        executorService.execute(server);
        
        server.getActivityWriter().write(new Activity("Started up.", Main.class));
        LOG.info("Graylog2 up and running.");

    }

    @Override
    public void stop() {
        if (server != null) {
            LOG.info("Graylog2 {} exiting.", Core.GRAYLOG2_VERSION);
            server.setOverallProcessing(false);
            executorService.shutdown();
        }
    }

    @Override
    public void destroy() {
        if (!executorService.isShutdown()) {
            stop();
        }
        server = null;
    }

}
