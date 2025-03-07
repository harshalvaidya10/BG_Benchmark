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

package janusgraph;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.StringByteIterator;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class JanusGraphClient extends DB{
	/** The code to return when the call succeeds. **/
	public static final int SUCCESS = 0;
	/** The code to return when the call fails. **/
	public static final int ERROR   = -1;
	/** Retry times. **/
	public static final int maxRetries = 5;
	public static final long sleepDuration = 50;
	private Properties props;
	private static volatile Client sharedClient = null;
	private static volatile GraphTraversalSource sharedG = null;
	private static volatile boolean initialized = false;
	private static final Object INIT_LOCK = new Object();

	private Client client;
	private GraphTraversalSource g;


	private int runWithRetry(Runnable operation) {
		for (int attempt = 1; attempt <= maxRetries; attempt++) {
			try {
				operation.run();
				System.out.println("Success while executing operation, attempt " + attempt + "/" + maxRetries);
				return SUCCESS;
			} catch (Exception e) {
				System.err.println("Error while executing operation, attempt " + attempt + "/" + maxRetries);
				e.printStackTrace();
				if (attempt < maxRetries) {
					try {
						Thread.sleep(sleepDuration);
					} catch (InterruptedException ie) {
						System.err.println("Sleep interrupted, aborting retries.");
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

	@Override
	public boolean init() throws DBException {
		// todo: reload everything
		if (!initialized) {
			synchronized (INIT_LOCK) {
				if (!initialized) {
					try {
						TypeSerializerRegistry registry = TypeSerializerRegistry.build()
								.addRegistry(JanusGraphIoRegistry.instance())
								.create();

						Cluster cluster = Cluster.build()
								.addContactPoint("128.110.96.123")
								.port(8182)
								.minConnectionPoolSize(10)
								.maxConnectionPoolSize(100)
								.maxSimultaneousUsagePerConnection(48)
								.maxWaitForConnection(5000)
								.serializer(new GraphBinaryMessageSerializerV1(registry))
								.maxContentLength(524288)
								.create();

						sharedClient = cluster.connect();
						sharedG = traversal().withRemote(DriverRemoteConnection.using(cluster));

						System.out.println("connected successfully in thread " + Thread.currentThread().getName());

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
		return true;
	}

	public static void main(String[] args) throws DBException {
		JanusGraphClient jclient = new JanusGraphClient();
		jclient.init();
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

		try {
			Map<String, Object> resultMap = g.V().hasLabel("users")
					.project("userCount", "minUserId", "avgFriendsPerUser", "avgPendingPerUser")
					.by(__.count())  // user_count
					.by(__.values("userid").min())  // offset
					.by(__.bothE("friendship").has("status", "friend").count())  // friendcount
					.by(__.bothE("friendship").has("status", "pending").count())  // pendingfriendcount
					.tryNext().orElse(null);

			if (resultMap == null) {
				stats.put("usercount", "0");
				stats.put("resourcesperuser", "0");
				stats.put("avgfriendsperuser", "0");
				stats.put("avgpendingperuser", "0");
				return stats;
			}

			// 获取统计结果
			stats.put("usercount", resultMap.getOrDefault("userCount", 0).toString());
			stats.put("resourcesperuser", "0");  // 资源数（此处为 0，可扩展）
			stats.put("avgfriendsperuser", resultMap.getOrDefault("avgFriendsPerUser", 0).toString());
			stats.put("avgpendingperuser", resultMap.getOrDefault("avgPendingPerUser", 0).toString());
			System.out.println(stats);


		} catch (Exception sx) {
			sx.printStackTrace(System.out);
		}
		return stats;
	}

	@Override
	public void cleanup(boolean warmup) throws ExecutionException, InterruptedException {
		try {
			g.V().drop().iterate();
			System.out.println("Graph database cleaned up.");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void createSchema(Properties props) {
		try {
			// read JSON file
			String schemaScript = new String(Files.readAllBytes(Paths.get("conf/schema.groovy")));
			sharedClient.submit(schemaScript).all().get();

			System.out.println("Schema successfully created!");
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
			if(entitySet.equalsIgnoreCase("resources")) {
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
			System.out.println("inserted successfully");
			return SUCCESS;
		} catch (Exception e) {
			System.err.println("Error while inserting entity into graph: " + entitySet);
			e.printStackTrace();
			return ERROR;
		}
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID){
		long timestamp = Instant.now().toEpochMilli(); // 毫秒级时间戳
		Runnable operation = () -> {
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

			System.out.println("[" + timestamp + "] " + "Friend request sent from " + inviterID + " -> " + inviteeID + " [Thread id: " + Thread.currentThread().getId() + "]");
		};

		return runWithRetry(operation);
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		Runnable operation = () -> {
			long timestamp = Instant.now().toEpochMilli();
			g.V().hasLabel("users").has("userid", friendid1).as("inviter")
					.V().hasLabel("users").has("userid", friendid2).as("invitee")
					.coalesce(__.select("inviter"), __.constant("Vertex with userid " + friendid1 + " not found"))
					.coalesce(__.select("invitee"), __.constant("Vertex with userid " + friendid2 + " not found"))
					.addE("friendship").from("inviter").to("invitee")
					.property("status", "friend")
					.iterate();

			System.out.println("[" + timestamp + "] " + "Friendship established from " + friendid1 + " -> " + friendid2 + " [Thread id: " + Thread.currentThread().getId() + "]");
		};
		return runWithRetry(operation);
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		// change the status of inviter and invitee into confirmed.
		Runnable operation = () -> {
				long timestamp = Instant.now().toEpochMilli(); // 毫秒级时间戳
				Long count = g.V().hasLabel("users").has("userid", inviterID)
						.outE("friendship").has("status", "pending")
						.where(__.inV().hasLabel("users").has("userid", inviteeID))
						.property("status", "friend")
						.count()
						.next();

				if (count == 0) {
					System.err.println("[" + timestamp + "] " + "Friendship accepted failed! From " + inviterID + " -> " + inviteeID + ". One or both vertices not found." + " [Thread id: " + Thread.currentThread().getId() + "]");
				} else if (count == 1) {
					System.out.println("[" + timestamp + "] " + "Friendship accepted from " + inviterID + " -> " + inviteeID + " [Thread id: " + Thread.currentThread().getId() + "]");
				} else{
					System.err.println("[" + timestamp + "] " + "Friendship accepted failed! From " + inviterID + " -> " + inviteeID + ". Multiple edges found." + " [Thread id: " + Thread.currentThread().getId() + "]");
				}
			};
		return runWithRetry(operation);
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		Runnable operation = () -> {
				long timestamp = Instant.now().toEpochMilli();
				Long count = g.V().hasLabel("users").has("userid", inviterID)
						.outE("friendship").has("status", "pending")
						.where(__.inV().hasLabel("users").has("userid", inviteeID))
						.property("status", "rejected")
						.count()
						.next();

				if (count == 0) {
					System.err.println("[" + timestamp + "] " + "Friendship rejected failed! From " + inviterID + " -> " + inviteeID + ". Didn't find any pending -> rejected edges" + " [Thread id: " + Thread.currentThread().getId() + "]");
				} else if (count == 1) {
					System.out.println("[" + timestamp + "] " + "Friendship rejected from " + inviterID + " -> " + inviteeID + " [Thread id: " + Thread.currentThread().getId() + "]");
				} else{
					System.err.println("[" + timestamp + "] " + "Friendship rejected failed! From " + inviterID + " -> " + inviteeID + ". Multiple edges found." + " [Thread id: " + Thread.currentThread().getId() + "]");
				}
			};
		return runWithRetry(operation);
	}


	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
						   HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {
			// get all the attributes
		try {
			long timestamp = Instant.now().toEpochMilli();
			Map<String, Object> resultMap = g.V().hasLabel("users").has("userid", profileOwnerID)
					.project("profile", "pendingFriendCount", "friendCount")
					.by(__.valueMap())
					.by(__.inE("friendship").has("status", "pending").count())
					.by(__.bothE("friendship").has("status", "friend").count())
					.tryNext().orElse(null);

			if (resultMap == null) {
				System.out.println(profileOwnerID + " can't find anything");
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
			System.out.println("[" + timestamp + "] " + "View Profile: " + "userid: " + result.get("userid") +" pendingcount: " + result.get("pendingcount") +" friendcount: " + result.get("friendcount") + " [Thread id: " + Thread.currentThread().getId() + "]");

			return SUCCESS;
		} catch (Exception e) {
			e.printStackTrace();
			return ERROR;
		}
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		Runnable operation = () -> {
			long timestamp = Instant.now().toEpochMilli();
				g.V().hasLabel("users").has("userid", friendid1)
						.bothE("friendship")
						.has("status", "friend")
						.where(__.otherV().hasLabel("users").has("userid", friendid2))
						.drop()
						.iterate();
				System.out.println("[" + timestamp + "] " + "Friendship thawed from " + friendid1 + " -> " + friendid2 + " [Thread id: " + Thread.currentThread().getId() + "]");
			};
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
			List<Map<Object, Object>> friends = g.V().hasLabel("users").has("userid", profileOwnerID)
					.bothE("friendship").has("status", "friend") // 获取已建立好友关系的边
					.otherV()
					.valueMap()
					.toList();

			for (Map<Object, Object> friendData : friends) {
				HashMap<String, ByteIterator> friendMap = new HashMap<>();

				if (fields != null) {
					for (String field : fields) {
						if (friendData.containsKey(field)) {
							Object value = friendData.get(field);
							if (value instanceof List && !((List<?>) value).isEmpty()) {
								friendMap.put(field, new StringByteIterator(((List<?>) value).get(0).toString()));
							}
						}
					}
				} else {
					friendData.forEach((key, value) -> {
						if (key instanceof String && value instanceof List) {
							List<?> valueList = (List<?>) value;
							if (!valueList.isEmpty()) {
								friendMap.put((String) key, new StringByteIterator(valueList.get(0).toString()));
							}
						}
					});
				}
				result.add(friendMap);
			}
			System.out.println("[" + timestamp + "] " + "View confirmed friendship, userid:" + profileOwnerID + " result: " + result.size() + " [Thread id: " + Thread.currentThread().getId() + "]");


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
			List<Map<Object, Object>> pendingRequests = g.V().hasLabel("users").has("userid", profileOwnerID)
					.inE("friendship").has("status", "pending") // 获取请求加好友的入边
					.outV()
					.valueMap()
					.toList();

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
			System.out.println("[" + timestamp + "] " + "View pending friendship, userid:" + profileOwnerID + " result: " + results.size() + " [Thread id: " + Thread.currentThread().getId() + "]");

			return SUCCESS;
		} catch (Exception e) {
			e.printStackTrace();
			return ERROR;
		}
	}


	@Override
	public int queryPendingFriendshipIds(int inviteeid, Vector<Integer> pendingIds){
		try {
			List<Object> pendingUserIds = g.V().hasLabel("users").has("userid", inviteeid)
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
			List<Object> confirmedUserIds = g.V().hasLabel("users").has("userid", profileId)
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
