package eu.leads.distsum;

import org.infinispan.ensemble.cache.EnsembleCache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.*;

import java.io.Serializable;

/**
 *
 * @author vagvaz
 * @author otrack
 *
 * Created by vagvaz on 7/5/14.
 *
 * Communication channel just just a map of String,Node to send messages
 */
public class ComChannel {

   private EnsembleCache<String,Message> nodes;

   public ComChannel(EnsembleCache<String,Message> c) {
      nodes =  c;
   }

   // Add a node to the map
   public void register(final String id, Node node){
      nodes.put(id, Message.EMPTYMSG); // to create the entry
      nodes.addClientListener(node, new Object[]{id}, null);
   }

   //Send messsage to node id
   public void sentTo(String id, Message message){
      nodes.put(id, message);
   }

   // Broadcast a message to all nodes, but coordinator
   // The coordinator takes as a result the replies of the nodes
   public void broadCast(Message message){
      for(String node: nodes.keySet()){
         if(!node.equals(Node.COORDINATOR)){
            nodes.put(node,message);
         }
      }
   }

   public static class ComChannelFilterFactory implements CacheEventFilterFactory, Serializable{
      @Override
      public <K, V> CacheEventFilter<K, V> getFilter(final Object[] params) {
         return new CacheEventFilter<K, V>() {
            @Override
            public boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata,
                  EventType eventType) {
               return params[0].equals(key);
            }
         };

      }
   }

   public static class ComChannelConverter implements CacheEventConverterFactory, Serializable{
      @Override
      public CacheEventConverter getConverter(Object[] params) {
         return new CacheEventConverter() {
            @Override 
            public Object convert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata,
                  EventType eventType) {
               return newValue;
            }
         };
      }
   }

}
