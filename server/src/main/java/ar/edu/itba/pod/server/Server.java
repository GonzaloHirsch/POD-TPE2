package ar.edu.itba.pod.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static Logger LOG = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        LOG.info("tpe2-g2 Server Starting ...");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
    }
}
