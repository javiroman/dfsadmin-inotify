package org.inotify;

import org.apache.hadoop.hdfs.inotify.Event;
import javax.json.Json;
import javax.json.JsonObject;
import java.util.regex.Pattern;

public class EventManagement {
    public String getJsonFormat(Event event, Event.EventType type, Long tid, String regex) {
        JsonObject json = null;

        switch (type) {
            case CREATE:
                Event.CreateEvent createEvent = (Event.CreateEvent) event;
                if (inSearchPath(regex, createEvent.getPath())) {
                    break;
                }
                json = Json.createObjectBuilder()
                        .add("TxId", tid)
                        .add("EventType", "CREATE")
                        .add("type", String.valueOf(createEvent.getiNodeType()))
                        .add("path", createEvent.getPath())
                        .add("ctime", createEvent.getCtime())
                        .add("ownerName", createEvent.getOwnerName())
                        .add("groupName", createEvent.getGroupName())
                        .add("perms", String.valueOf(createEvent.getPerms()))
                        .add("replication", createEvent.getReplication())
                        .add("overwrite", createEvent.getOverwrite()).build();
                break;
            case CLOSE:
                Event.CloseEvent closeEvent = (Event.CloseEvent) event;
                if (inSearchPath(regex, closeEvent.getPath())) {
                    break;
                }
                json = Json.createObjectBuilder()
                        .add("TxId", tid)
                        .add("EventType", "CLOSE")
                        .add("path", closeEvent.getPath())
                        .add("fileSize", closeEvent.getFileSize())
                        .add("timestamp", closeEvent.getTimestamp()).build();
                break;
            case APPEND:
                Event.AppendEvent appendEvent = (Event.AppendEvent) event;
                if (inSearchPath(regex, appendEvent.getPath())) {
                    break;
                }
                json = Json.createObjectBuilder()
                        .add("TxId", tid)
                        .add("EventType", "APPEND")
                        .add("path", appendEvent.getPath()).build();
                break;
            case RENAME:
                Event.RenameEvent renameEvent = (Event.RenameEvent) event;
                if (inSearchPath(regex, renameEvent.getSrcPath())) {
                    break;
                }
                json = Json.createObjectBuilder()
                        .add("TxId", tid)
                        .add("EventType", "RENAME")
                        .add("srcPath", renameEvent.getSrcPath())
                        .add("destPath", renameEvent.getDstPath())
                        .add("timestamp", renameEvent.getTimestamp()).build();
                break;
            case METADATA:
                Event.MetadataUpdateEvent metadataUpdateEvent = (Event.MetadataUpdateEvent) event;
                if (inSearchPath(regex, metadataUpdateEvent.getPath())) {
                    break;
                }
                json = Json.createObjectBuilder()
                        .add("TxId", tid)
                        .add("EventType", "METADATA")
                        .add("path", metadataUpdateEvent.getPath())
                        .add("type", String.valueOf(metadataUpdateEvent.getMetadataType()))
                        .add("mtime", metadataUpdateEvent.getMtime())
                        .add("atime", metadataUpdateEvent.getAtime())
                        .add("perms", String.valueOf(metadataUpdateEvent.getPerms())).build();
                break;
            case UNLINK:
                Event.UnlinkEvent unlinkEvent = (Event.UnlinkEvent) event;
                if (inSearchPath(regex, unlinkEvent.getPath())) {
                    break;
                }
                json = Json.createObjectBuilder()
                        .add("TxId", tid)
                        .add("EventType", "UNLINK")
                        .add("path", unlinkEvent.getPath())
                        .add("timestamp", unlinkEvent.getTimestamp()).build();
                break;
            case TRUNCATE:
                Event.TruncateEvent truncateEvent = (Event.TruncateEvent) event;
                if (inSearchPath(regex, truncateEvent.getPath())) {
                    break;
                }
                json = Json.createObjectBuilder()
                        .add("TxId", tid)
                        .add("EventType", "TRUNCATE")
                        .add("path", truncateEvent.getPath())
                        .add("fileSize", truncateEvent.getFileSize())
                        .add("timestamp", truncateEvent.getTimestamp()).build();
                break;
            default:
                json = Json.createObjectBuilder()
                        .add("TxId", tid)
                        .add("EventType", "UNKNOWN").build();
                break;
        }

        if (json != null) {
            return json.toString();
        } else {
            return "{}";
        }
    }

    public boolean inSearchPath(String regexSearchPath, String eventPath) {
        return  Pattern.matches(regexSearchPath, eventPath);

    }
}
