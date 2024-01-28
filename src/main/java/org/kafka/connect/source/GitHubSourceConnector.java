package org.kafka.connect.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.kafka.connect.source.config.ConnectorConfiguration;
import org.slf4j.impl.VersionUtil;

import java.util.*;

public class GitHubSourceConnector extends SourceConnector {

    private ConnectorConfiguration config;

    @Override
    public void start(Map<String, String> map) {
        config = new ConnectorConfiguration(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GitHubSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        configs.add(config.originalsStrings());
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return ConnectorConfiguration.getConfig();
    }

    @Override
    public String version() {
        try {
            return VersionUtil.class.getPackage().getImplementationVersion();
        } catch(Exception ex){
            return null;
        }
    }
}
