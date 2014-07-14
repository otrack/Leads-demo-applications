package eu.leads.distsum;

import java.io.Serializable;

/**
 *
 * @author vagvaz
 * @author otrack
 *
 * Created by vagvaz on 7/5/14.
 */
public class Message implements Serializable {

    String from;   //Who sends the message
    String type;   //Type of Message
    Object body;   //The body of message

    public static final  Message EMPTYMSG = new Message("","");

    public Message() {
    }

    public Message(String from,String type){
        this.from = from;
        this.type = type;
    }

    /**
     * Getter for property 'from'.
     *
     * @return Value for property 'from'.
     */
    public String getFrom() {
        return from;
    }

    /**
     * Setter for property 'from'.
     *
     * @param from Value to set for property 'from'.
     */
    public void setFrom(String from) {
        this.from = from;
    }

    public Message(String from,String type,Object body){
        this.from = from;

        this.type =type;
        this.body = body;
    }

    /**
     * Getter for property 'type'.
     *
     * @return Value for property 'type'.
     */
    public String getType() {
        return type;
    }

    /**
     * Setter for property 'type'.
     *
     * @param type Value to set for property 'type'.
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Getter for property 'body'.
     *
     * @return Value for property 'body'.
     */
    public Object getBody() {
        return body;
    }

    /**
     * Setter for property 'body'.
     *
     * @param body Value to set for property 'body'.
     */
    public void setBody(Object body) {
        this.body = body;
    }
}
