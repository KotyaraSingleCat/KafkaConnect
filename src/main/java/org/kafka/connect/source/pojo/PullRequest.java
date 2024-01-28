
package org.kafka.connect.source.pojo;

import lombok.*;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@With
@Getter
public class PullRequest {

    private String url;
    private String htmlUrl;
    private String diffUrl;
    private String patchUrl;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    public static PullRequest fromJson(JSONObject pull_request) {
        return new PullRequest()
                .withUrl(pull_request.getString(GitHubConstants.PR_URL_FIELD))
                .withHtmlUrl(pull_request.getString(GitHubConstants.PR_HTML_URL_FIELD));

    }
}
