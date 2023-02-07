package org.apache.seatunnel.connectors.doris.domain.rest;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Column implements Serializable {
    private String name;
    private String aggregation_type;
    private String comment;
    private String type;
}