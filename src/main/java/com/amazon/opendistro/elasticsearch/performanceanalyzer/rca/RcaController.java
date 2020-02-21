/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.MetricsDBProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ThresholdMain;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.ReceivedFlowUnitStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.PublishRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.SubscribeServerHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.NetPersistor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistenceFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCAScheduler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RcaSchedulerState;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rest.QueryRcaRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.ThreadProvider;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is responsible for bootstrapping the RCA. It sets the necessary state, initializes the
 * pollers to periodically check for the state of the system and react to a change if necessary. It
 * does all the preparations so that RCA runtime and graph evaluation can be started with a
 * configuration change but without needing a process restart.
 */
public class RcaController {

  private static final Logger LOG = LogManager.getLogger(RcaController.class);

  private static final String RCA_ENABLED_CONF_FILE = "rca_enabled.conf";
  private static final int ONE_MINUTE_IN_MILLIS = 60 * 1000;

  private final ScheduledExecutorService netOpsExecutorService;
  private final boolean useHttps;

  private boolean rcaEnabledDefaultValue = false;

  // This needs to be volatile as the RcaConfPoller writes it but the Nanny reads it.
  private volatile boolean rcaEnabled = false;

  // This needs to be volatile as the NodeRolePoller writes it but the Nanny reads it.
  private volatile NodeRole currentRole = NodeRole.UNKNOWN;

  private RCAScheduler rcaScheduler;

  private NetPersistor netPersistor;
  private NetClient rcaNetClient;
  private NetServer rcaNetServer;
  private NodeStateManager nodeStateManager;
  private HttpServer httpServer;
  private QueryRcaRequestHandler queryRcaRequestHandler;

  private SubscriptionManager subscriptionManager;

  private final String RCA_ENABLED_CONF_LOCATION;
  private final int rcaStateCheckIntervalMillis;
  private final int roleCheckPeriodicity;

  // Atomic reference to the networking threadpool as it is used by multiple threads. When we
  // replace the threadpool instance, we want the update to be visible to all others holding a
  // reference.
  private AtomicReference<ExecutorService> networkThreadPoolReference = new AtomicReference<>();
  private ReceivedFlowUnitStore receivedFlowUnitStore;

  public RcaController(
      final ScheduledExecutorService netOpsExecutorService,
      final GRPCConnectionManager grpcConnectionManager,
      final ClientServers clientServers,
      final String rca_enabled_conf_location,
      final int rcaStateCheckIntervalMillis) {
    this.netOpsExecutorService = netOpsExecutorService;
    this.rcaNetClient = clientServers.getNetClient();
    this.rcaNetServer = clientServers.getNetServer();
    this.httpServer = clientServers.getHttpServer();
    RCA_ENABLED_CONF_LOCATION = rca_enabled_conf_location;
    netPersistor = new NetPersistor();
    this.useHttps = PluginSettings.instance().getHttpsEnabled();
    subscriptionManager = new SubscriptionManager(grpcConnectionManager);
    nodeStateManager = new NodeStateManager();
    queryRcaRequestHandler = new QueryRcaRequestHandler();
    this.rcaScheduler = null;
    this.rcaStateCheckIntervalMillis = rcaStateCheckIntervalMillis;
    this.roleCheckPeriodicity = ONE_MINUTE_IN_MILLIS / rcaStateCheckIntervalMillis;
  }

  private void start() {
    final RcaConf rcaConf = RcaControllerHelper.pickRcaConfForRole(currentRole);
    try {
      subscriptionManager.setCurrentLocus(rcaConf.getTagMap().get("locus"));
      List<ConnectedComponent> connectedComponents = RcaUtil.getAnalysisGraphComponents(rcaConf);
      Queryable db = new MetricsDBProvider();
      ThresholdMain thresholdMain = new ThresholdMain(RcaConsts.THRESHOLDS_PATH, rcaConf);
      Persistable persistable = PersistenceFactory.create(rcaConf);
      networkThreadPoolReference
          .set(RcaControllerHelper.buildNetworkThreadPool(rcaConf.getNetworkQueueLength()));
      addRcaRequestHandler();
      queryRcaRequestHandler.setPersistable(persistable);
      receivedFlowUnitStore = new ReceivedFlowUnitStore(rcaConf.getPerVertexBufferLength());
      WireHopper net =
          new WireHopper(nodeStateManager, rcaNetClient, subscriptionManager,
              networkThreadPoolReference, receivedFlowUnitStore);
      this.rcaScheduler =
          new RCAScheduler(connectedComponents, db, rcaConf, thresholdMain, persistable, net);

      rcaNetServer.setSendDataHandler(new PublishRequestHandler(
          nodeStateManager, receivedFlowUnitStore, networkThreadPoolReference));
      rcaNetServer.setSubscribeHandler(
          new SubscribeServerHandler(subscriptionManager, networkThreadPoolReference));

      rcaScheduler.setRole(currentRole);
      Thread rcaSchedulerThread = ThreadProvider.instance()
                                                .createThreadForRunnable(() -> rcaScheduler.start(),
                                                    "rca-scheduler");

      rcaSchedulerThread.start();
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException
        | MalformedConfig
        | SQLException
        | IOException e) {
      LOG.error("Couldn't build connected components or persistable.. Ran into {}", e.getMessage());
      e.printStackTrace();
    }
  }

  private void stop() {
    rcaScheduler.shutdown();
    rcaNetClient.stop();
    rcaNetServer.stop();
    receivedFlowUnitStore.drainAll();
    networkThreadPoolReference.get().shutdown();
    try {
      networkThreadPoolReference.get().awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOG.warn("Awaiting termination interrupted. {}", e.getCause(), e);
      networkThreadPoolReference.get().shutdownNow();
      // Set the thread's interrupt interrupt flag so that higher level interrupt handlers can
      // take appropriate actions.
      Thread.currentThread().interrupt();
    }
    removeRcaRequestHandler();
    Stats.getInstance().reset();
  }

  private void restart() {
    stop();
    start();
    StatsCollector.instance().logMetric(RcaConsts.RCA_SCHEDULER_RESTART_METRIC);
  }

  public void run() {
    int tick = 1;
    while (true) {
      try {
        long startTime = System.currentTimeMillis();
        readRcaEnabledFromConf();
        if (rcaEnabled && tick % roleCheckPeriodicity == 0) {
          tick = 0;
          final NodeDetails nodeDetails = ClusterDetailsEventProcessor.getCurrentNodeDetails();
          if (nodeDetails != null) {
            checkUpdateNodeRole(nodeDetails);
          }
        }

        updateRcaState();
        long duration = System.currentTimeMillis() - startTime;
        if (duration < rcaStateCheckIntervalMillis) {
          Thread.sleep(rcaStateCheckIntervalMillis - duration);
        }
      } catch (InterruptedException ie) {
        LOG.error("RCA controller thread was interrupted. Reason: {}", ie.getMessage());
        LOG.error(ie);
        break;
      }
      tick++;
    }
  }

  private void checkUpdateNodeRole(final NodeDetails currentNode) {
    final NodeRole currentNodeRole = NodeRole.valueOf(currentNode.getRole());
    Boolean isMasterNode = currentNode.getIsMasterNode();
    if (isMasterNode != null) {
      currentRole = isMasterNode ? NodeRole.ELECTED_MASTER : currentNodeRole;
    } else {
      final String electedMasterHostAddress = RcaControllerHelper.getElectedMasterHostAddress();
      currentRole = currentNode.getHostAddress().equalsIgnoreCase(electedMasterHostAddress)
          ? NodeRole.ELECTED_MASTER : currentNodeRole;
    }
  }

  /**
   * Reads the enabled/disabled value for RCA from the conf file.
   */
  private void readRcaEnabledFromConf() {
    Path filePath = Paths.get(RCA_ENABLED_CONF_LOCATION, RCA_ENABLED_CONF_FILE);

    Util.invokePrivileged(
        () -> {
          try (Scanner sc = new Scanner(filePath)) {
            String nextLine = sc.nextLine();
            rcaEnabled = Boolean.parseBoolean(nextLine);
          } catch (IOException e) {
            LOG.error("Error reading file '{}': {}", filePath.toString(), e.getMessage());
            e.printStackTrace();
            rcaEnabled = rcaEnabledDefaultValue;
          }
        });
  }

  /**
   * Starts or stops the RCA runtime. If the RCA runtime is up but the currently RCA is disabled,
   * then this gracefully shuts down the RCA runtime. It restarts the RCA runtime if the node role
   * has changed in the meantime (such as a new elected master). It also starts the RCA runtime if
   * it wasn't already running but the current state of the flag expects it to.
   */
  private void updateRcaState() {
    if (rcaScheduler != null && rcaScheduler.getState() == RcaSchedulerState.STATE_STARTED) {
      if (!rcaEnabled) {
        // Need to shutdown the rca scheduler
        stop();
        PerformanceAnalyzerApp.RCA_RUNTIME_METRICS_AGGREGATOR.updateStat(
            RcaRuntimeMetrics.RCA_STOPPED_BY_OPERATOR, "", 1);
      } else {
        if (rcaScheduler.getRole() != currentRole) {
          restart();
          PerformanceAnalyzerApp.RCA_RUNTIME_METRICS_AGGREGATOR.updateStat(
              RcaRuntimeMetrics.RCA_RESTARTED_BY_OPERATOR, "", 1);
        }
      }
    } else {
      // Start the scheduler if all the following conditions are met:
      // 1. rca is enabled
      // 2. we know the role of this es node
      // 3. scheduler is not stopped due to an exception.
      if (rcaEnabled && NodeRole.UNKNOWN != currentRole && (rcaScheduler == null
          || rcaScheduler.getState() != RcaSchedulerState.STATE_STOPPED_DUE_TO_EXCEPTION)) {
        start();
      }
    }
  }


  private void removeRcaRequestHandler() {
    try {
      httpServer.removeContext(Util.RCA_QUERY_URL);
    } catch (IllegalArgumentException e) {
      LOG.debug("Http(s) context for path: {} was not found to remove.", Util.RCA_QUERY_URL);
    }
  }

  public static String getCatMasterUrl() {
    return RcaControllerHelper.CAT_MASTER_URL;
  }

  public static String getRcaEnabledConfFile() {
    return RCA_ENABLED_CONF_FILE;
  }

  public boolean isRcaEnabled() {
    return rcaEnabled;
  }

  public NodeRole getCurrentRole() {
    return currentRole;
  }

  public RCAScheduler getRcaScheduler() {
    return rcaScheduler;
  }

  private void addRcaRequestHandler() {
    httpServer.createContext(Util.RCA_QUERY_URL, queryRcaRequestHandler);
  }
}
