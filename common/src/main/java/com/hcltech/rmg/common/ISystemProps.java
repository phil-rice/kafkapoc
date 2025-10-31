package com.hcltech.rmg.common;

import java.util.Map;

public interface ISystemProps {
    String getProperty(String key);

    String getProperty(String key, String defaultValue);

    static ISystemProps mock(Map<String,String> map){
        return new ISystemProps() {
            @Override
            public String getProperty(String key) {
                return map.get(key);
            }

            @Override
            public String getProperty(String key, String defaultValue) {
                return map.getOrDefault(key, defaultValue);
            }
        };
    }

    ISystemProps real = new ISystemProps() {
        @Override
        public String getProperty(String key) {
            return System.getProperty(key);
        }

        @Override
        public String getProperty(String key, String defaultValue) {
            return System.getProperty(key, defaultValue);
        }
    };
}
