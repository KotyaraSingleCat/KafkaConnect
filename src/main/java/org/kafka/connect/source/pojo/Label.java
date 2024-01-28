
package org.kafka.connect.source.pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class Label {

    private Integer id;
    private String url;
    private String name;
    private String color;
    private Boolean _default;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

}
