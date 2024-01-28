
package org.kafka.connect.source.pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class User {

    private String login;
    private Integer id;
    private String avatarUrl;
    private String gravatarId;
    private String url;
    private String htmlUrl;
    private String followersUrl;
    private String followingUrl;
    private String gistsUrl;
    private String starredUrl;
    private String subscriptionsUrl;
    private String organizationsUrl;
    private String reposUrl;
    private String eventsUrl;
    private String receivedEventsUrl;
    private String type;
    private Boolean siteAdmin;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    public static User fromJson(JSONObject jsonObject) {
        User user = new User();
        user.setUrl(jsonObject.getString(GitHubConstants.USER_URL_FIELD));
        user.setHtmlUrl(jsonObject.getString(GitHubConstants.USER_HTML_URL_FIELD));
        user.setId(jsonObject.getInt(GitHubConstants.USER_ID_FIELD));
        user.setLogin(jsonObject.getString(GitHubConstants.USER_LOGIN_FIELD));
        return user;
    }
}
