// 回滚当前事务（如果有）
graph.tx().rollback()

// 打开管理接口
mgmt = graph.openManagement()

userIndexExists = mgmt.getGraphIndex('user_id_index') != null
if (!userIndexExists) {
    println "Composite index 'user_id_index' does not exist. Creating new index..."
    userId = mgmt.getPropertyKey("userid") ?: mgmt.makePropertyKey("userid").dataType(Integer.class).make()
    users = mgmt.getVertexLabel('users') ?: mgmt.makeVertexLabel("users").make()
    mgmt.buildIndex('user_id_index', Vertex.class)
            .addKey(userId)
            .unique()
            .indexOnly(users)  // 限制索引仅适用于 users 顶点
            .buildCompositeIndex()
    mgmt.commit()

    println "Waiting for composite index 'user_id_index' to become available..."
    ManagementSystem.awaitGraphIndexStatus(graph, 'user_id_index').status(SchemaStatus.ENABLED).call()

    println "Reindexing existing data for composite index 'user_id_index'..."
    mgmt = graph.openManagement()
    mgmt.updateIndex(mgmt.getGraphIndex('user_id_index'), SchemaAction.REINDEX).get()
    mgmt.commit()
} else {
    println "Composite index 'user_id_index' already exists. Skipping creation..."
}

mgmt = graph.openManagement()
friendship = mgmt.getEdgeLabel("friendship") ?: mgmt.makeEdgeLabel("friendship").make()

status = mgmt.getPropertyKey("status") ?: mgmt.makePropertyKey("status").dataType(String.class).make()

friendshipIndexExists = mgmt.getRelationIndex(friendship, 'friendship_status_index') != null
if (!friendshipIndexExists) {
    println "Vertex-centric index 'friendship_status_index' does not exist. Creating new index..."

    mgmt.buildEdgeIndex(friendship, 'friendship_status_index', Direction.BOTH, Order.asc, status)
    mgmt.commit()

    println "Waiting for vertex-centric index 'friendship_status_index' to become available..."
    ManagementSystem.awaitRelationIndexStatus(graph, 'friendship_status_index', 'friendship').status(SchemaStatus.ENABLED).call()

    println "Reindexing existing data for vertex-centric index 'friendship_status_index'..."
    mgmt = graph.openManagement()
    mgmt.updateIndex(mgmt.getRelationIndex(friendship, 'friendship_status_index'), SchemaAction.REINDEX).get()
    mgmt.commit()
} else {
    println "Vertex-centric index 'friendship_status_index' already exists. Skipping creation..."
}

println "Index creation and reindexing completed successfully!"