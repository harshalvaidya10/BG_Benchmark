/**
 * Copyright (c) 2012 USC Database Laboratory All rights reserved.
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package JanusGraph.src.janusgraph;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.StringByteIterator;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

import org.nugraph.client.config.AbstractNuGraphConfig;
import org.nugraph.client.config.NuGraphConfigManager;
import org.nugraph.client.gremlin.driver.remote.NuGraphClientException;
import org.nugraph.client.gremlin.process.traversal.dsl.graph.RemoteNuGraphTraversalSource;
//import org.nugraph.client.gremlin.driver.remote.GrpcConnectionPool.createNewConnections;
import org.nugraph.client.gremlin.driver.remote.Options;
import org.nugraph.client.gremlin.driver.remote.ReadMode;
import org.nugraph.client.gremlin.structure.RemoteNuGraph;
public class JanusGraphClient extends DB{

	public static class DefaultLoggableOperation implements LoggableOperation {
		private final List<String> logs = new ArrayList<>();
		private final Runnable task;
		private final String operationId;

		public DefaultLoggableOperation(String operationId, Runnable task) {
			this.operationId = operationId;
			this.task = task;
		}

		@Override
		public void run() {
			logs.clear(); // clear old logs
			task.run();
		}

		@Override
		public List<String> getLogs() {
			return logs;
		}

		public void addLog(String log) {
			logs.add(log);
		}

		public String getOperationId() {
			return operationId;
		}
	}


	/** The code to return when the call succeeds. **/
	public static final int SUCCESS = 0;
	/** The code to return when the call fails. **/
	public static final int ERROR   = -1;
	/** Retry times. **/
	public static final int maxRetries = 10;
	public static final long sleepDuration = 50;
	public boolean cache = true;
	private Properties props;
	private static volatile Client sharedClient = null;
	private static volatile GraphTraversalSource sharedG = null;
	private static volatile GraphTraversalSource sharedGRead = null;
	private static volatile boolean initialized = false;
	private static final Object INIT_LOCK = new Object();
	private static final Logger logger = Logger.getLogger(JanusGraphClient.class.getName());
	private Client client;
	private GraphTraversalSource g;
	private GraphTraversalSource g_read;
	boolean showProfile = false;

	private void logCacheMetrics(TraversalMetrics metrics) {
		String metricStr = metrics.toString();
		Pattern patternHits = Pattern.compile("_cacheHits=(\\d+)");
		Pattern patternMisses = Pattern.compile("_cacheMisses=(\\d+)");
		Pattern patternTmp = Pattern.compile("_template=([^\\n]+)");
		Matcher matcherHits = patternHits.matcher(metricStr);
		String cacheHits = matcherHits.find() ? matcherHits.group(1) : "N/A";
		Matcher matcherMisses = patternMisses.matcher(metricStr);
		Matcher matcherTemplate = patternTmp.matcher(metricStr);
		String cacheMisses = matcherMisses.find() ? matcherMisses.group(1) : "N/A";
		String template = matcherTemplate.find() ? matcherTemplate.group(1).trim() : "N/A";
		String logMsg = "[Cache Metrics] Step: JanusGraphCacheStep"
				+ ", CacheHits: " + cacheHits
				+ ", CacheMisses: " + cacheMisses
				+ ", Template: " + template;
		logger.warning(logMsg);

	}

	private <T> List<T> runTraversalWithProfile(Supplier<Traversal<?, T>> querySupplier, boolean showProfile) {
		List<T> results = querySupplier.get().toList();
		if (showProfile) {
			Traversal<?, TraversalMetrics> tProfile = querySupplier.get().profile();
			List<TraversalMetrics> metricsList = tProfile.toList();
			if (!metricsList.isEmpty()) {
				TraversalMetrics metrics = metricsList.get(0);
				logCacheMetrics(metrics);
			} else {
				logger.warning("No traversal metrics available.");
			}
		}
		return results;
	}


	private int runWithRetry(DefaultLoggableOperation operation) {
		String opId = operation.getOperationId();
		for (int attempt = 1; attempt <= maxRetries; attempt++) {
			try {
				operation.run();
				List<String> logs = operation.getLogs();
				if (!logs.isEmpty()) {
					if (attempt > 3) {
						logger.warning(String.format("[Attempt %d/%d] [Thread %d] %s", attempt, maxRetries, Thread.currentThread().getId(), logs.get(0)));
					} else {
						logger.info(String.format("[Attempt %d/%d] [Thread %d] %s", attempt, maxRetries, Thread.currentThread().getId(), logs.get(0)));
					}
				}
				return SUCCESS;
			} catch (Exception e) {
				logger.severe(String.format("[Operation %s] [Thread %d] Attempt %d/%d failed: %s",
						opId, Thread.currentThread().getId(), attempt, maxRetries, e.getMessage()));
				if (attempt < maxRetries) {
					try {
						Thread.sleep(sleepDuration);
					} catch (InterruptedException ie) {
						logger.severe("Sleep interrupted, aborting retries.");
						ie.printStackTrace();
						return ERROR;
					}
				} else {
					return ERROR;
				}
			}

		}
		return ERROR;
	}
	public class CustomNuGraphConfig extends AbstractNuGraphConfig {
		private final String serviceIp;
		private final String failoverServiceIp;
		private final boolean ssl;
		private final String tlsAuthorityOverride;

		public CustomNuGraphConfig(String serviceHost, String failoverIp,
								   boolean ssl, String tlsAuthorityOverride) {
			this.serviceIp = serviceHost;
			this.failoverServiceIp = failoverIp;
			this.ssl = ssl;
			this.tlsAuthorityOverride = tlsAuthorityOverride;
		}

		@Override
		public String getFailoverServiceHost() {
			return failoverServiceIp;
		}

		@Override
		public String getRegion() {
			return "NA";
		}

		@Override
		public int getRestConnectTimeout() {
			return 1000;
		}

		@Override
		public int getRestReadTimeout() {
			return 1000;
		}

		@Override
		public String getServiceHost() {
			return serviceIp;
		}

		@Override
		public int getServicePort() {
			return ssl ? 8443: 8080;
		}

		@Override
		public long getTimeout() {
			return 20000;
		}

		@Override
		public boolean isPublishMetricsToSherlock() {
			return false;
		}

		@Override
		public boolean isResetTimeoutOnRetryEnabled() {
			return false;
		}

		@Override
		public boolean isSSLEnabled() {
			return ssl;
		}

		@Override
		public String getProperty(PropertyName prop) {
			if (prop.equals(PropertyName.UMP_EVENTS_ENABLED)) {
				return "false";
			} else if (prop.equals(PropertyName.UMP_NAMESPACE)) {
				return "nugraph";
			} else if (prop.equals(PropertyName.UMP_CONSUMERID)) {
				return "urn:ebay-marketplace-consumerid:33110598-3c00-4901-9887-2de13a5f1e9c";
			} else if (prop.equals(PropertyName.AUTHORITY_OVERRIDE)) {
				return tlsAuthorityOverride;
			}

			return super.getProperty(prop);
		}
	}
	public synchronized GraphTraversalSource getInstance(String hostName,
																 String authOverride, String keyspace, boolean isread) {
		GraphTraversalSource gg;
		try {
			AbstractNuGraphConfig config = new CustomNuGraphConfig(
					hostName, hostName, true, authOverride
			);
			NuGraphConfigManager.setDefaultConfigAndInit("YCSB", config);

			// 创建远程图遍历源
			HashMap<String, Object> optionsMap = new HashMap<>();
			optionsMap.put(Options.TIMEOUT_IN_MILLIS, 99999);
			optionsMap.put(Options.IS_RETRY_ALLOWED, true);
			if(isread) {
				optionsMap.put(Options.READ_MODE, ReadMode.READ_SNAPSHOT);
				optionsMap.put(Options.READ_ONLY, true);
			}
			gg = RemoteNuGraph.instance().traversal().withRemote(keyspace, optionsMap);

		} catch (NuGraphClientException e) {
			throw new RuntimeException(e);
		}
		return gg;
	}
	@Override
	public boolean init() throws DBException {
		// todo: reload everything
		props = getProperties();
		cache = Boolean.parseBoolean(props.getProperty("doCache", "true"));
		logger.setLevel(Level.WARNING);
		if (!initialized) {
			synchronized (INIT_LOCK) {
				if (!initialized) {
					try {
//						TypeSerializerRegistry registry = TypeSerializerRegistry.build()
//								.addRegistry(JanusGraphIoRegistry.instance())
//								.create();
//
//						Cluster cluster = Cluster.build()
//								.addContactPoint("128.110.96.75")
//								.port(8182)
//								.minConnectionPoolSize(10)
//								.maxConnectionPoolSize(100)
//								.maxSimultaneousUsagePerConnection(48)
//								.maxWaitForConnection(5000)
//								.serializer(new GraphBinaryMessageSerializerV1(registry))
//								.maxContentLength(524288)
//								.create();

//						sharedClient = cluster.connect();
//						sharedG = traversal().withRemote(DriverRemoteConnection.using(cluster));
						sharedG = getInstance("nugraphservice-testyimingfdbntypesbg-lvs-internal.vip.ebay.com",
								"nugraphservice-slc.monstor-preprod.svc.23.tess.io", "ldbc_sf_01_b", false);
						sharedGRead = getInstance("nugraphservice-testyimingfdbntypesbg-lvs-internal.vip.ebay.com",
								"nugraphservice-slc.monstor-preprod.svc.23.tess.io", "ldbc_sf_01_b", true);
						logger.info("connected successfully in thread " + Thread.currentThread().getName());

						try {
							createSchema(props);
						} catch (Exception e) {
							e.printStackTrace(System.out);
						}
						initialized = true;
					} catch (Exception e) {
						e.printStackTrace(System.out);
						return false;
					}
				}
			}
		}

		this.client = sharedClient;
		this.g = sharedG;
		this.g_read = sharedGRead;
		return true;
	}

	public static void main(String[] args) throws DBException {
		JanusGraphClient jclient = new JanusGraphClient();
		jclient.init();
		jclient.viewProfile(0, 0, new HashMap<>(), false, false);
		jclient.listFriends(0, 0, null, new Vector<>(), false, false);
	}

	private void cleanupAllConnections() {
		try {
			if (client != null) {
				client.close();
				g.close();
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		HashMap<String, String> stats = new HashMap<String, String>();

//		try {
//			Map<String, Object> resultMap = g.V().hasLabel("users").has("userid", 0)
//					.project("friendCount", "pendingCount")
//					.by(__.outE("friendship").has("status", "friend").count())
//					.by(__.outE("friendship").has("status", "pending").count())
//					.tryNext().orElse(null);
			stats.put("usercount", "100000");
			stats.put("resourcesperuser", "0");
			stats.put("avgfriendsperuser", "5");
			stats.put("avgpendingperuser", "0");
			return stats;
//			if (resultMap == null) {
//				stats.put("usercount", "0");
//				stats.put("resourcesperuser", "0");
//				stats.put("avgfriendsperuser", "0");
//				stats.put("avgpendingperuser", "0");
//				return stats;
//			}
//
//			// 获取统计结果
//			int userCount = Math.toIntExact(g.V().hasLabel("users").count().next());
//			stats.put("usercount", String.valueOf(userCount));
//			stats.put("resourcesperuser", "0");  // 资源数（此处为 0，可扩展）
//			int avgF = ((Long) resultMap.getOrDefault("avgFriendsPerUser", 0L)).intValue();
//			int avgP = ((Long) resultMap.getOrDefault("avgPendingPerUser", 0L)).intValue();
//			stats.put("avgfriendsperuser", String.valueOf(avgF));
//			stats.put("avgpendingperuser", String.valueOf(avgP));
//
//		} catch (Exception sx) {
//			sx.printStackTrace(System.out);
//		}
//		return stats;
	}

	@Override
	public void cleanup(boolean warmup) throws ExecutionException, InterruptedException {
		try {
			// cleanup all connections
			cleanupAllConnections();
//			g.V().drop().iterate();
			logger.info("Graph database cleaned up.");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void createSchema(Properties props) {
		try {
			// read JSON file
//			String schemaScript = new String(Files.readAllBytes(Paths.get("conf/schema.groovy")));
//			sharedClient.submit(schemaScript).all().get();

			logger.info("Schema successfully created!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}



	@Override
	public int insertEntity(String entitySet, String entityPK,
							HashMap<String, ByteIterator> values, boolean insertImage) {
		// entityPK is automaticly generated?
		if (entitySet == null) {
			return -1;
		}
		if (entityPK == null) {
			return -1;
		}
		ResultSet rs =null;

		try {
			if(entitySet.equalsIgnoreCase("main/resources")) {
				return SUCCESS; // TODO: at this stage we don't add any resourse
			}
			GraphTraversal<Vertex, Vertex> traversal = g.addV(entitySet)
					.property("userid", Integer.parseInt(entityPK));

			values.forEach((key, value) -> {
				if (!key.equalsIgnoreCase("pic") && !key.equalsIgnoreCase("tpic")) {
					String cleaned = value.toString().replace("'", "\\'");
					traversal.property(key.toLowerCase(), cleaned);
				}
			});
			traversal.next();
			logger.info("inserted successfully");
			return SUCCESS;
		} catch (Exception e) {
			logger.severe("Error while inserting entity into graph: " + entitySet);
			e.printStackTrace();
			return SUCCESS;
		}
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID){
		long timestamp = Instant.now().toEpochMilli();
		String operationId = String.format("inviteFriend-%d-%d-%d", inviterID, inviteeID, timestamp);
		DefaultLoggableOperation operation = new DefaultLoggableOperation(operationId, () -> {
				g.V().hasLabel("users").has("userid", inviterID).as("inviter")
						.V().hasLabel("users").has("userid", inviteeID).as("invitee")
						.coalesce(__.select("inviter"), __.constant("Vertex with userid " + inviterID + " not found"))
						.coalesce(__.select("invitee"), __.constant("Vertex with userid " + inviteeID + " not found"))
						// if friendship edge exists and status is rejected
						.sideEffect(
								__.select("inviter")
										.outE("friendship")
										.where(__.inV().as("invitee"))
										.has("status", "rejected")
										.property("status", "pending")
						)
						// if friendship edge not exists
						.choose(
								__.select("inviter").outE("friendship").where(__.inV().as("invitee")),
								__.identity(),
								__.addE("friendship").from("inviter").to("invitee").property("status", "pending")
						)
						.iterate();
		});
		operation.addLog("[" + timestamp + "] " + "Friend request sent from " + inviterID + " -> " + inviteeID + " [Thread id: " + Thread.currentThread().getId() + "]");
		return runWithRetry(operation);
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		long timestamp = Instant.now().toEpochMilli();
		String operationId = String.format("CreateFriendship-%d-%d-%d", friendid1, friendid2, timestamp);
		DefaultLoggableOperation operation = new DefaultLoggableOperation(operationId, () -> {
				g.V().hasLabel("users").has("userid", friendid1).as("inviter")
						.V().hasLabel("users").has("userid", friendid2).as("invitee")
						.coalesce(__.select("inviter"), __.constant("Vertex with userid " + friendid1 + " not found"))
						.coalesce(__.select("invitee"), __.constant("Vertex with userid " + friendid2 + " not found"))
						.addE("friendship").from("inviter").to("invitee")
						.property("status", "friend")
						.iterate();
		});
		operation.addLog("[" + timestamp + "] " + "Friendship established from " + friendid1 + " -> " + friendid2 + " [Thread id: " + Thread.currentThread().getId() + "]");
		return runWithRetry(operation);
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		// change the status of inviter and invitee into confirmed.
		long timestamp = Instant.now().toEpochMilli();
		String operationId = String.format("acceptFriend-%d-%d-%d", inviterID, inviteeID, timestamp);
		AtomicLong count = new AtomicLong(-1L);
		DefaultLoggableOperation operation = new DefaultLoggableOperation(operationId, () -> {
				count.set(g.V().hasLabel("users").has("userid", inviterID)
						.outE("friendship").has("status", "pending")
						.where(__.inV().hasLabel("users").has("userid", inviteeID))
						.property("status", "friend")
						.count()
						.next());
			});
		if (count.get() == 0) {
			operation.addLog("[" + timestamp + "] " + "Friendship accepted failed! From " + inviterID + " -> " + inviteeID + ". One or both vertices not found." + " [Thread id: " + Thread.currentThread().getId() + "]");
		} else if (count.get() == 1) {
			operation.addLog("[" + timestamp + "] " + "Friendship accepted from " + inviterID + " -> " + inviteeID + " [Thread id: " + Thread.currentThread().getId() + "]");
		} else {
			operation.addLog("[" + timestamp + "] " + "Friendship accepted failed! From " + inviterID + " -> " + inviteeID + ". Multiple edges found." + " [Thread id: " + Thread.currentThread().getId() + "]");
		}
		return runWithRetry(operation);
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		long timestamp = Instant.now().toEpochMilli();
		String operationId = String.format("rejectFriend-%d-%d-%d", inviterID, inviteeID, timestamp);
		AtomicLong count = new AtomicLong(-1L);
		DefaultLoggableOperation operation = new DefaultLoggableOperation(operationId, () -> {
				count.set(g.V().hasLabel("users").has("userid", inviterID)
						.outE("friendship").has("status", "pending")
						.where(__.inV().hasLabel("users").has("userid", inviteeID))
						.property("status", "rejected")
						.count()
						.next());
			});
		if (count.get() == 0) {
			operation.addLog("[" + timestamp + "] " + "Friendship rejected failed! From " + inviterID + " -> " + inviteeID + ". Didn't find any pending -> rejected edges" + " [Thread id: " + Thread.currentThread().getId() + "]");
		} else if (count.get() == 1) {
			operation.addLog("[" + timestamp + "] " + "Friendship rejected from " + inviterID + " -> " + inviteeID + " [Thread id: " + Thread.currentThread().getId() + "]");
		} else{
			operation.addLog("[" + timestamp + "] " + "Friendship rejected failed! From " + inviterID + " -> " + inviteeID + ". Multiple edges found." + " [Thread id: " + Thread.currentThread().getId() + "]");
		}
		return runWithRetry(operation);
	}


	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
						   HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {
		// get all the attributes
		try {
			long timestamp = Instant.now().toEpochMilli();
			GraphTraversalSource traversal = g_read.with("cache", cache);
			List<Map<String, Object>> resultsList = runTraversalWithProfile(() ->
							traversal.V().hasLabel("users").has("userid", profileOwnerID)
									.project("profile", "pendingFriendCount", "friendCount")
									.by(__.valueMap())
									.by(__.inE("friendship").has("status", "pending").outV().count())
									.by(__.bothE("friendship").has("status", "friend").otherV().count()),
					showProfile
			);

			Map<String, Object> resultMap = resultsList.isEmpty() ? null : resultsList.get(0);
			if (resultMap == null) {
				logger.info(profileOwnerID + " can't find anything");
				return SUCCESS;
			}

			Map<Object, Object> valueMap = (Map<Object, Object>) resultMap.get("profile");
			valueMap.forEach((key, value) -> {
				if (key instanceof String && value instanceof List) {
					List<?> valueList = (List<?>) value;
					if (!valueList.isEmpty()) {
						result.put((String) key, new StringByteIterator(valueList.get(0).toString()));
					}
				}
			});
			long pendingFriendCount = (long) resultMap.get("pendingFriendCount");
			long friendCount = (long) resultMap.get("friendCount");

			result.put("pendingcount", new StringByteIterator(String.valueOf(pendingFriendCount)));
			result.put("friendcount", new StringByteIterator(String.valueOf(friendCount)));
			logger.info("[" + timestamp + "] " + "View Profile: " + "userid: " + result.get("userid") +" pendingcount: " + result.get("pendingcount") +" friendcount: " + result.get("friendcount") + " [Thread id: " + Thread.currentThread().getId() + "]");

			return SUCCESS;
		} catch (Exception e) {
			e.printStackTrace();
			return ERROR;
		}
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		long timestamp = Instant.now().toEpochMilli();
		String operationId = String.format("rejectFriend-%d-%d-%d", friendid1, friendid2, timestamp);
		DefaultLoggableOperation operation = new DefaultLoggableOperation(operationId, () -> {
				g.V().hasLabel("users").has("userid", friendid1)
						.bothE("friendship")
						.has("status", "friend")
						.where(__.otherV().hasLabel("users").has("userid", friendid2))
						.drop()
						.iterate();
				});
		operation.addLog("[" + timestamp + "] " + "Friendship thawed from " + friendid1 + " -> " + friendid2 + " [Thread id: " + Thread.currentThread().getId() + "]");
		return runWithRetry(operation);
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
						   Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
						   boolean insertImage, boolean testMode) {
		// gets the list of friends for a member.
		if (requesterID < 0 || profileOwnerID < 0)
			return ERROR;
		try {
			long timestamp = Instant.now().toEpochMilli();
			GraphTraversalSource traversal = g_read.with("cache", cache);
			List<Map<Object, Object>> friends;

			if (fields != null && fields.size() > 0) {
				friends = runTraversalWithProfile(() ->
								traversal.V().hasLabel("users").has("userid", profileOwnerID)
										.bothE("friendship").has("status", "friend")
										.otherV()
										.valueMap(fields.toArray(new String[0])),
						showProfile
				);
			} else {
				friends = runTraversalWithProfile(() ->
								traversal.V().hasLabel("users").has("userid", profileOwnerID)
										.bothE("friendship").has("status", "friend")
										.otherV()
										.valueMap("userid", "fname", "lname"),
						showProfile
				);
			}

			for (Map<Object, Object> friendData : friends) {
				HashMap<String, ByteIterator> friendMap = new HashMap<>();
				friendData.forEach((key, value) -> {
					if (value instanceof List<?> && !((List<?>) value).isEmpty()) {
						friendMap.put(key.toString(), new StringByteIterator(((List<?>) value).get(0).toString()));
					}
				});
				result.add(friendMap);
			}

			logger.info("[" + timestamp + "] " + "View confirmed friendship, userid:" + profileOwnerID +
					" result: " + result.size() + " [Thread id: " + Thread.currentThread().getId() + "]");
			return SUCCESS;
		} catch (Exception e) {
			e.printStackTrace();
			return ERROR;
		}
	}

	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String,ByteIterator>> results, boolean insertImage, boolean testMode) {
		// gets the list of pending friend requests for a member.
		try {
			long timestamp = Instant.now().toEpochMilli();
			GraphTraversalSource traversal = g_read.with("cache", cache);
			List<Map<Object, Object>> pendingRequests = runTraversalWithProfile(() ->
							traversal.V().hasLabel("users").has("userid", profileOwnerID)
									.inE("friendship").has("status", "pending")
									.outV()
									.valueMap("userid", "fname", "lname"),
					showProfile
			);

			for (Map<Object, Object> friendData : pendingRequests) {
				HashMap<String, ByteIterator> friendMap = new HashMap<>();
				friendData.forEach((key, value) -> {
					if (key instanceof String && value instanceof List) {
						List<?> valueList = (List<?>) value;
						if (!valueList.isEmpty()) {
							friendMap.put((String) key, new StringByteIterator(valueList.get(0).toString()));
						}
					}
				});
				results.add(friendMap);
			}
			logger.info("[" + timestamp + "] " + "View pending friendship, userid:" + profileOwnerID + " result: " + results.size() + " [Thread id: " + Thread.currentThread().getId() + "]");

			return SUCCESS;
		} catch (Exception e) {
			e.printStackTrace();
			return ERROR;
		}
	}


	@Override
	public int queryPendingFriendshipIds(int inviteeid, Vector<Integer> pendingIds){
		try {
			GraphTraversalSource traversal = g_read.with("cache", cache);
			List<Object> pendingUserIds = traversal.V().hasLabel("users").has("userid", inviteeid)
					.inE("friendship").has("status", "pending")
					.outV().values("userid")
					.toList();

			for (Object id : pendingUserIds) {
				pendingIds.add(Integer.parseInt(id.toString()));
			}

			return SUCCESS;
		} catch (Exception e) {
			e.printStackTrace();
			return ERROR;
		}
	}

	@Override
	public int queryConfirmedFriendshipIds(int profileId, Vector<Integer> confirmedIds){
		try {
			GraphTraversalSource traversal = g_read.with("cache", cache);
			List<Object> confirmedUserIds = traversal.V().hasLabel("users").has("userid", profileId)
					.inE("friendship").has("status", "friend")
					.outV().values("userid")
					.toList();

			for (Object id : confirmedUserIds) {
				confirmedIds.add(Integer.parseInt(id.toString()));
			}

			return SUCCESS;
	} catch (Exception e) {
		e.printStackTrace();
		return ERROR;
	}
	}


	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
								 Vector<HashMap<String, ByteIterator>> result) {return SUCCESS;}

	@Override
	public int getCreatedResources(int creatorID,
								   Vector<HashMap<String, ByteIterator>> result) {return SUCCESS;}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
									 int resourceID, Vector<HashMap<String, ByteIterator>> result) {return SUCCESS;}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
									 int resourceID, HashMap<String, ByteIterator> values) {return SUCCESS;}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
									int manipulationID) {return SUCCESS;}



}