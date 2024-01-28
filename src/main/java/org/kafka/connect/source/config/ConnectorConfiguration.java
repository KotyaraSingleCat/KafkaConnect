package org.kafka.connect.source.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.kafka.connect.source.validators.BatchSizeValidator;
import org.kafka.connect.source.validators.TimestampValidator;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;

public class ConnectorConfiguration extends AbstractConfig {

    public static final String TOPIC_CONFIG = "topic.name";
    private static final String TOPIC_DOC = "Name of topic in which data will write.";

    public static final String OWNER_CONFIG = "github.repository.owner";
    private static final String OWNER_DOC = "Owner of the repository you'd like to follow.";

    public static final String REPO_CONFIG = "github.repository";
    private static final String REPO_DOC = "Repository you'd like to follow.";

    public static final String SINCE_CONFIG = "since.timestamp";
    private static final String SINCE_DOC =
            "Only issues updated at or after this time are returned.\n"
                    + "This is a timestamp in ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ.\n"
                    + "Defaults to a year from first launch.";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "Number of data points to retrieve at a time. Defaults to 250 (max value)";

    public static final String AUTH_USERNAME_CONFIG = "auth.username";
    private static final String AUTH_USERNAME_DOC = "Optional Username to authenticate calls.";

    public static final String AUTH_PASSWORD_CONFIG = "auth.password";
    private static final String AUTH_PASSWORD_DOC = "Optional Password to authenticate calls.";


    public ConnectorConfiguration(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public ConnectorConfiguration(Map<?, ?> originals) {
        this(getConfig(), originals);
    }

    public static ConfigDef getConfig() {
        return new ConfigDef()
                .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, TOPIC_DOC)
                .define(OWNER_CONFIG, Type.STRING, Importance.HIGH, OWNER_DOC)
                .define(REPO_CONFIG, Type.STRING, Importance.HIGH, REPO_DOC)
                .define(BATCH_SIZE_CONFIG, Type.INT, 250, new BatchSizeValidator(), Importance.LOW, BATCH_SIZE_DOC)
                .define(SINCE_CONFIG, Type.STRING, ZonedDateTime.now().minusYears(1).toInstant().toString(), new TimestampValidator(), Importance.HIGH, SINCE_DOC)
                .define(AUTH_USERNAME_CONFIG, Type.STRING, "", Importance.HIGH, AUTH_USERNAME_DOC)
                .define(AUTH_PASSWORD_CONFIG, Type.PASSWORD, "", Importance.HIGH, AUTH_PASSWORD_DOC);
    }

    public String getOwnerConfig() {
        return this.getString(OWNER_CONFIG);
    }

    public String getRepoConfig() {
        return this.getString(REPO_CONFIG);
    }

    public Integer getBatchSize() {
        return this.getInt(BATCH_SIZE_CONFIG);
    }

    public Instant getSince() {
        return Instant.parse(this.getString(SINCE_CONFIG));
    }

    public String getTopic() {
        return this.getString(TOPIC_CONFIG);
    }

    public String getAuthUsername() {
        return this.getString(AUTH_USERNAME_CONFIG);
    }

    public String getAuthPassword(){
        return this.getPassword(AUTH_PASSWORD_CONFIG).value();
    }
}