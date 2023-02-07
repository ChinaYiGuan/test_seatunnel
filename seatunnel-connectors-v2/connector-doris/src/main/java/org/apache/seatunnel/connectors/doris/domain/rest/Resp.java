package org.apache.seatunnel.connectors.doris.domain.rest;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Resp  implements Serializable {

    private String msg;
    private int code;
    private Object data;
}
