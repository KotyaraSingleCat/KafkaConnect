package org.kafka.connect.source;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kafka.connect.source.config.ConnectorConfiguration;
import org.kafka.connect.source.pojo.Issue;
import org.kafka.connect.source.pojo.PullRequest;
import org.kafka.connect.source.pojo.User;
import org.kafka.connect.source.pojo.GitHubConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.VersionUtil;

import java.time.Instant;
import java.util.*;

public class GitHubSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(GitHubSourceTask.class);
    private ConnectorConfiguration config;
    protected Instant nextQuerySince;
    protected Integer lastIssueNumber;
    protected Integer nextPageToVisit = 1;
    protected Instant lastUpdatedAt;

    public Map<String, Object> lastSourceOffset;

    private GitHubAPIHttpClient gitHubHttpAPIClient;

    @Override
    public String version() {
        try {
            return VersionUtil.class.getPackage().getImplementationVersion();
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public void start(Map<String, String> map) {
        config = new ConnectorConfiguration(map);
        initializeLastVariables();
        gitHubHttpAPIClient = new GitHubAPIHttpClient(config);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        gitHubHttpAPIClient.sleepIfNeed();

        // fetch data
        final ArrayList<SourceRecord> records = new ArrayList<>();
        JSONArray issues = gitHubHttpAPIClient.getNextIssues(nextPageToVisit, nextQuerySince);
        // we'll count how many results we get with i
        int i = 0;
        for (Object obj : issues) {
            Issue issue = Issue.fromJson((JSONObject) obj);
            SourceRecord sourceRecord = generateSourceRecord(issue);
            records.add(sourceRecord);
            i += 1;
            lastUpdatedAt = issue.getUpdatedAt();
        }
        if (i > 0) log.info(String.format("Fetched %s record(s)", i));
        if (i == 250){
            // we have reached a full batch, we need to get the next one
            nextPageToVisit += 1;
        }
        else {
            nextQuerySince = lastUpdatedAt.plusSeconds(1);
            nextPageToVisit = 1;
            gitHubHttpAPIClient.sleep();
        }
        return records;
    }

    @Override
    public void stop() {

    }

    private void initializeLastVariables(){
        String jsonOffsetData = new Gson().toJson(lastSourceOffset);
        // TypeToken preserves the generic type information of the Map when deserializing the JSON string.
        TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {};
        Map<String, Object> offsetData = new Gson().fromJson(jsonOffsetData, typeToken.getType());
        if( offsetData == null){
            // we haven't fetched anything yet, so we initialize to 7 days ago
            nextQuerySince = config.getSince();
            lastIssueNumber = -1;
        } else {
            Object updatedAt = offsetData.get(GitHubConstants.UPDATED_AT_FIELD);
            Object issueNumber = offsetData.get(GitHubConstants.NUMBER_FIELD);
            Object nextPage = offsetData.get(GitHubConstants.NEXT_PAGE_FIELD);
            if((updatedAt instanceof String)){
                nextQuerySince = Instant.parse((String) updatedAt);
            }
            if((issueNumber instanceof String)){
                lastIssueNumber = Integer.valueOf((String) issueNumber);
            }
            if ((nextPage instanceof String)){
                nextPageToVisit = Integer.valueOf((String) nextPage);
            }
        }
    }

    private SourceRecord generateSourceRecord(Issue issue) {
        return new SourceRecord(
                sourcePartition(),
                sourceOffset(issue.getUpdatedAt()),
                config.getTopic(),
                null, // partition will be inferred by the framework
                GitHubConstants.KEY_SCHEMA,
                buildRecordKey(issue),
                GitHubConstants.VALUE_SCHEMA,
                buildRecordValue(issue),
                issue.getUpdatedAt().toEpochMilli());
    }

    private Map<String, String> sourcePartition() {
        Map<String, String> map = new HashMap<>();
        map.put(GitHubConstants.OWNER_FIELD, config.getOwnerConfig());
        map.put(GitHubConstants.REPOSITORY_FIELD, config.getRepoConfig());
        return map;
    }

    private Map<String, String> sourceOffset(Instant updatedAt) {
        Map<String, String> map = new HashMap<>();
        map.put(GitHubConstants.UPDATED_AT_FIELD, maxInstant(updatedAt, nextQuerySince).toString());
        map.put(GitHubConstants.NEXT_PAGE_FIELD, nextPageToVisit.toString());
        return map;
    }

    private Struct buildRecordKey(Issue issue){
        Struct key = new Struct(GitHubConstants.KEY_SCHEMA)
                .put(GitHubConstants.OWNER_FIELD, config.getOwnerConfig())
                .put(GitHubConstants.REPOSITORY_FIELD, config.getRepoConfig())
                .put(GitHubConstants.NUMBER_FIELD, issue.getNumber());

        return key;
    }

    public Struct buildRecordValue(Issue issue){

        Struct valueStruct = new Struct(GitHubConstants.VALUE_SCHEMA)
                .put(GitHubConstants.URL_FIELD, issue.getUrl())
                .put(GitHubConstants.TITLE_FIELD, issue.getTitle())
                .put(GitHubConstants.CREATED_AT_FIELD, Date.from(issue.getCreatedAt()))
                .put(GitHubConstants.UPDATED_AT_FIELD, Date.from(issue.getUpdatedAt()))
                .put(GitHubConstants.NUMBER_FIELD, issue.getNumber())
                .put(GitHubConstants.STATE_FIELD, issue.getState());

        User user = issue.getUser();
        Struct userStruct = new Struct(GitHubConstants.USER_SCHEMA)
                .put(GitHubConstants.USER_URL_FIELD, user.getUrl())
                .put(GitHubConstants.USER_ID_FIELD, user.getId())
                .put(GitHubConstants.USER_LOGIN_FIELD, user.getLogin());
        valueStruct.put(GitHubConstants.USER_FIELD, userStruct);

        PullRequest pullRequest = issue.getPullRequest();
        if (pullRequest != null) {
            Struct prStruct = new Struct(GitHubConstants.PR_SCHEMA)
                    .put(GitHubConstants.PR_URL_FIELD, pullRequest.getUrl())
                    .put(GitHubConstants.PR_HTML_URL_FIELD, pullRequest.getHtmlUrl());
            valueStruct.put(GitHubConstants.PR_FIELD, prStruct);
        }

        return valueStruct;
    }

    private Instant maxInstant (Instant i1, Instant i2){
        return i1.compareTo(i2) > 0 ? i1 : i2;
    }
}
