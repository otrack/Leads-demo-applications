package eu.leads.distsum;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author vagvaz
 * @author otrack

 * Created by vagvaz on 7/5/14.
 */

@ClientListener(filterFactoryName = "comchannel-factory", converterFactoryName = "comchannel-factory")
public abstract class Node {

   public static final String COORDINATOR = "COORDINATOR";
   private static final int MAX_ENTRIES = 1000;
   public String id;
   private HashMap<UUID,Integer> received; // singularity of eventing system

   public Node(String i, ComChannel channel){
      id = i;
      received = new LinkedHashMap<UUID, Integer>(MAX_ENTRIES + 1, .75F, false) {
         protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > MAX_ENTRIES;
         }
      };
      channel.register(id,this);
   }

   public abstract void receiveMessage(Message msg);


   @SuppressWarnings({ "rawtypes", "unchecked" })
   @ClientCacheEntryModified
   public void onCacheModification(ClientCacheEntryCustomEvent event){
      Message m = (Message) event.getEventData();
      if (received.containsKey(m.id))
         return;
      received.put(m.id,0);
      this.receiveMessage(m);
   }

}
