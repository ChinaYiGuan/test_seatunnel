package org.apache.seatunnel.api.datasource;

import org.apache.seatunnel.api.common.PluginIdentifierInterface;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.util.List;


public interface BaseDataSource extends Serializable, PluginIdentifierInterface {

    List<? extends Config> convertCfgs(List<? extends Config> cfgs);

    default List<? extends Config> defCvtCfgs(List<? extends Config> cfgs) {
        return cfgs;
    }

}
