
package org.kafka.connect.source.pojo;

import lombok.*;
import org.json.JSONObject;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.kafka.connect.source.pojo.GitHubConstants.*;


@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class Issue {

    private Integer id;
    private String url;
    private String repositoryUrl;
    private String labelsUrl;
    private String commentsUrl;
    private String eventsUrl;
    private String htmlUrl;
    private Integer number;
    private String state;
    private String title;
    private String body;
    private User user;
    private List<Label> labels = null;
    private Assignee assignee;
    private Milestone milestone;
    private Boolean locked;
    private Integer comments;
    private PullRequest pullRequest;
    private Object closedAt;
    private Instant createdAt;
    private Instant updatedAt;
    private List<Assignee> assignees = null;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    public static Issue fromJson(JSONObject jsonObject) {
        Issue issue = new Issue();
        issue.setUrl(jsonObject.getString(URL_FIELD));
        issue.setHtmlUrl(jsonObject.getString(HTML_URL_FIELD));
        issue.setTitle(jsonObject.getString(TITLE_FIELD));
        issue.setCreatedAt(Instant.parse(jsonObject.getString(CREATED_AT_FIELD)));
        issue.setUpdatedAt(Instant.parse(jsonObject.getString(UPDATED_AT_FIELD)));
        issue.setNumber(jsonObject.getInt(NUMBER_FIELD));
        issue.setState(jsonObject.getString(STATE_FIELD));

        User user = User.fromJson(jsonObject.getJSONObject(USER_FIELD));
        issue.setUser(user);

        if (jsonObject.has(PR_FIELD)){
            PullRequest pullRequest = PullRequest.fromJson(jsonObject.getJSONObject(PR_FIELD));
            issue.setPullRequest(pullRequest);
        }

        return issue;
    }
}
