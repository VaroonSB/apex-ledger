package com.apex.ledger.infrastructure.persistence.converter;

import com.apex.ledger.domain.model.IdempotencyKey;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/** Maps {@link IdempotencyKey} to {@code transactions.idempotency_key}. */
@Converter
public class IdempotencyKeyConverter implements AttributeConverter<IdempotencyKey, String> {

    @Override
    public String convertToDatabaseColumn(IdempotencyKey attribute) {
        return attribute == null ? null : attribute.value();
    }

    @Override
    public IdempotencyKey convertToEntityAttribute(String dbData) {
        return dbData == null ? null : IdempotencyKey.of(dbData);
    }
}
