package com.apex.ledger.infrastructure.persistence.converter;

import com.apex.ledger.domain.model.RequestFingerprint;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/** Maps {@link RequestFingerprint} to {@code transactions.request_fingerprint}. */
@Converter
public class RequestFingerprintConverter
        implements AttributeConverter<RequestFingerprint, String> {

    @Override
    public String convertToDatabaseColumn(RequestFingerprint attribute) {
        return attribute == null ? null : attribute.value();
    }

    @Override
    public RequestFingerprint convertToEntityAttribute(String dbData) {
        return dbData == null ? null : RequestFingerprint.ofHex(dbData);
    }
}
