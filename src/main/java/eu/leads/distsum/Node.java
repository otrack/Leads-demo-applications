package eu.leads.distsum;

/**
 *
 * @author vagvaz
 * @author otrack

 * Created by vagvaz on 7/5/14.
 */

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;

@Listener(clustered = true, sync = false)
public abstract class Node {

    public static final String COORDINATOR = "COORDINATOR";
    public String id;

    public Node(String i, ComChannel channel){
        id = i;
        channel.register(id,this);
    }

    public abstract void receiveMessage(Message msg);


    @SuppressWarnings({ "rawtypes", "unchecked" })
    @CacheEntryModified
    public void onCacheModification(CacheEntryEvent event){
        if (event.getKey().equals(id))
            this.receiveMessage((Message) event.getValue());
    }


}
