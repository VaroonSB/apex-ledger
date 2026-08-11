package com.apex.ledger.infrastructure.persistence.converter;

import com.apex.ledger.domain.model.CurrencyCode;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/**
 * Maps {@link CurrencyCode} to the {@code VARCHAR(3)} currency columns.
 *
 * <p>Not {@code autoApply}: applied explicitly with {@code @Convert} so it is obvious at each field
 * which columns carry a validated currency.
 *
 * <p>Reading runs the value back through {@link CurrencyCode}'s constructor, so a row that somehow
 * holds an invalid code fails loudly on load rather than propagating into the domain.
 */
@Converter
public class CurrencyCodeConverter implements AttributeConverter<CurrencyCode, String> {

    @Override
    public String convertToDatabaseColumn(CurrencyCode attribute) {
        return attribute == null ? null : attribute.code();
    }

    @Override
    public CurrencyCode convertToEntityAttribute(String dbData) {
        return dbData == null ? null : CurrencyCode.of(dbData);
    }
}
