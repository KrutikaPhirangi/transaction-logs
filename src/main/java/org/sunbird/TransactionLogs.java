package org.sunbird;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import org.janusgraph.core.*;

import org.janusgraph.core.log.ChangeState;
import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.core.log.TransactionId;


import java.time.Instant;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;


public class TransactionLogs {
    public static void listenLogsEvent() throws Exception {
        GraphTraversalSource g = traversal().withRemote("/Users/sanketika-mac1/Documents/GitHub/transaction-logs/src/main/conf/remote-graph.properties");
        JanusGraph graph = JanusGraphFactory.open("/Users/sanketika-mac1/Documents/GitHub/transaction-logs/src/main/conf/janusgraph-inmemory.properties");

        LogProcessorFramework logProcessor = JanusGraphFactory.openTransactionLog(graph);
        logProcessor.addLogProcessor("TestLog")
                .setProcessorIdentifier("NCF")
                .setStartTime(Instant.now())
                .addProcessor((JanusGraphTransaction janusGraphTransaction, TransactionId transactionId, ChangeState changeState) -> {
                    System.out.println("tx--" + janusGraphTransaction);
                    System.out.println("txId--" + transactionId);
                    System.out.println("changeState--" + changeState);
                })
                .build();

        System.out.println("adding the data");
        try {
            g.addV().property("type", "Content011");
            graph.tx().commit();
            System.out.println("Vertex committed ");
            System.out.println("Transaction committed successfully");
        } catch (Exception e) {
            System.out.println("Error committing transaction: " + e.getMessage());
            e.printStackTrace();
            graph.tx().rollback();
        } finally {
            graph.close();
        }
        System.out.println("---LIST-----");
        System.out.println(g.V().valueMap().toList());
        g.V().hasLabel("NCF").valueMap().toList().forEach(System.out::println);

    }

    public static void main(String[] args) throws Exception {
        System.out.println("starting main");
        listenLogsEvent();
        System.out.println("ENDED");
    }
}

