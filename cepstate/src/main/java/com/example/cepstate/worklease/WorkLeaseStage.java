package com.example.cepstate.worklease;

/**
 * Represents a work lease stage in a process.
 * @param name - the name of the state. Just for logging.
 * @param retryTimeOutMs - Wait this long before retrying the work lease.
 * @param jitterms - Add random jitter, max this, to the retry time out.
 * @param giveUpTopic - If not null... this is the end of the line and we post the message to the giveUpTopic.
 */
public record WorkLeaseStage(String name, Integer retryTimeOutMs, Integer jitterms, String giveUpTopic) {
}
