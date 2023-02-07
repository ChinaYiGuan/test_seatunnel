package org.apache.seatunnel.common.tsf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TsfMeta implements Serializable {
    private String identifier;
    private LocalDateTime time;
    private List<String> names;
    private List<String> types;
}
