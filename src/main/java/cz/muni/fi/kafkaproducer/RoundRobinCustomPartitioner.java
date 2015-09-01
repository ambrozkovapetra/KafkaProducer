package cz.muni.fi.kafkaproducer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;


public class RoundRobinCustomPartitioner implements Partitioner {
    int counter = 0;
    
    public RoundRobinCustomPartitioner (VerifiableProperties props) {
             
    }

    @Override
    public int partition(Object obj, int partitions) {
       int partitionId = counter++ % partitions;
       if (counter == partitions) {
            counter = 0;
       }
       return partitionId;
    }
}
