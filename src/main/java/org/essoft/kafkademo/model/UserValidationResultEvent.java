package org.essoft.kafkademo.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class UserValidationResultEvent {
    private String userId;
    private Boolean isValid;
}
