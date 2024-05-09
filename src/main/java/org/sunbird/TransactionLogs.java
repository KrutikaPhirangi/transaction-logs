package org.sunbird;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import org.janusgraph.core.*;

import org.janusgraph.core.log.ChangeProcessor;
import org.janusgraph.core.log.ChangeState;
import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.core.log.TransactionId;


import java.time.Instant;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;


public class TransactionLogs {
    public static void listenLogsEvent() throws Exception {

        JanusGraph graph = JanusGraphFactory.open("conf/janusgraph-inmemory.properties");
        LogProcessorFramework logProcessor = JanusGraphFactory.openTransactionLog(graph);

        logProcessor.addLogProcessor("TestLog").
                setProcessorIdentifier("NCF").
                setStartTime(Instant.now()).
                addProcessor(new ChangeProcessor() {
                    @Override
                    public void process(JanusGraphTransaction tx, TransactionId txId, ChangeState changeState) {
                        System.out.println("tx " + tx.tx());
                        System.out.println("txId " + txId.getInstanceId());
                        System.out.println("changeState " + changeState);
                    }
                }).
                build();
        JanusGraphTransaction tx = graph.buildTransaction().logIdentifier("TestLog").start();
        try {
            for (int i = 0; i <= 10; i++) {
                System.out.println("going to add =" + i);
                JanusGraphVertex a = tx.addVertex("NCF_099");
                a.property("type", "HOLD");
                a.property("serialNo", "XS31B4");
                tx.commit();
                System.out.println("Vertex committed =" + a);
            }
        } catch (Exception e) {
            e.printStackTrace();
            tx.rollback();
        } finally {
            tx.close();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("starting main");
        listenLogsEvent();
        System.out.println("ENDED");
    }
}

