package org.apache.seatunnel.connectors.doris.domain.rest;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SchemaResp implements Serializable {
    private String keysType;
    private Integer status;
    private List<Column> properties;

}
