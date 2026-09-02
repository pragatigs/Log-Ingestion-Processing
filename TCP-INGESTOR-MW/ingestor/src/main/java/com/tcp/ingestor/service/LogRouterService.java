package com.tcp.ingestor.service;

import java.util.regex.Pattern;

import org.springframework.stereotype.Service;

@Service
public class LogRouterService {

    private static final Pattern INFO_PATTERN =
            Pattern.compile("info", Pattern.CASE_INSENSITIVE);

    private static final Pattern DEBUG_PATTERN =
            Pattern.compile("debug", Pattern.CASE_INSENSITIVE);

    private static final Pattern ERROR_PATTERN =
            Pattern.compile("error", Pattern.CASE_INSENSITIVE);

    private static final Pattern WARN_PATTERN =
            Pattern.compile("warn", Pattern.CASE_INSENSITIVE);

    public int decidePartition(String log) {

        if (INFO_PATTERN.matcher(log).find()) {
            return 0;
        } else if (DEBUG_PATTERN.matcher(log).find()) {
            return 1;
        } else if (ERROR_PATTERN.matcher(log).find()) {
            return 2;
        } else if (WARN_PATTERN.matcher(log).find()) {
            return 3;
        } else {
            return 4;
        }
    }
}
