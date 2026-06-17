package com.taken_seat.coupon_service.infrastructure.kafka;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;

public enum QuantityUpdatedResponseType {
    NO_QUANTITY(-1L),
    ALREADY_APPLIED(-2L),
    SUCCESS(0L);

    final Long number;

    QuantityUpdatedResponseType(Long number) {
        this.number = number;
    }

    public static Optional<QuantityUpdatedResponseType> valueOf(Long number) {
        return EnumSet.allOf(QuantityUpdatedResponseType.class)
                .stream()
                .filter(t -> Objects.equals(t.number, number))
                .findFirst();
    }
}
