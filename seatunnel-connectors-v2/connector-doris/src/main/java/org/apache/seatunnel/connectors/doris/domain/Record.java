package org.apache.seatunnel.connectors.doris.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Record implements Serializable {

    private String identifier;
    private String fullTableName;
    private String dataJson;
}
