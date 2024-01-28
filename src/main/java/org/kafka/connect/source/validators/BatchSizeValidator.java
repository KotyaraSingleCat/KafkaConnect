package org.kafka.connect.source.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class BatchSizeValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
        try {
            int batchSize = (int) value;
            if(batchSize > 250 || batchSize < 1) {
                throw new ConfigException(name, value, "Batch Size must be a positive integer that's less or equal to 250");
            }
        } catch (ClassCastException e) {
            throw new ConfigException(name, value, "Incorrect value was set in batch.size! Please enter integer value.");
        }
    }
}
