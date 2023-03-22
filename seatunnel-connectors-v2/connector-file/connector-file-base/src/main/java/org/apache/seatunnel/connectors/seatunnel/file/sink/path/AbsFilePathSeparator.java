package org.apache.seatunnel.connectors.seatunnel.file.sink.path;

import java.io.File;

public abstract class AbsFilePathSeparator {

    public String separator() {
        return File.separator;
    }

}
