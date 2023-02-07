package org.apache.seatunnel.common.tsf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TsfData implements Serializable {
    private String name;
    private String type;
    private Object value;
}