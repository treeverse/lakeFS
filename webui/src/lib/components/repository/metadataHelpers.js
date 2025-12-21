const isEmptyKey = (key) => !key || key.trim() === "";

const isInvalidKey = (key) => {
    // TODO: Add more validation checks, e.g. duplicate keys, invalid characters
    return isEmptyKey(key);
};

/**
 * Checks if there are any invalid keys in the metadata fields.
 * @param {Array<{key: string, value: string, touched: boolean}>} metadataFields
 * @returns {boolean} True if there are any invalid keys
 */
export const hasInvalidKeys = (metadataFields) => {
    return metadataFields.some((f) => isInvalidKey(f.key));
};

/**
 * Converts metadata fields array to a plain key-value object.
 * @param {Array<{key: string, value: string, touched: boolean}>} metadataFields
 * @returns {Object} Metadata object with key-value pairs
 */
export const fieldsToMetadata = (metadataFields) => {
    const metadata = {};
    metadataFields.forEach((pair) => (metadata[pair.key] = pair.value));
    return metadata;
};

/**
 * Returns a new array with invalid fields marked as touched.
 * Does not mutate the input array.
 * @param {Array<{key: string, value: string, touched: boolean}>} metadataFields
 * @returns {Array<{key: string, value: string, touched: boolean}>} New array with invalid fields touched
 */
export const touchInvalidFields = (metadataFields) => {
    return metadataFields.map((field) =>
        isInvalidKey(field.key) ? { ...field, touched: true } : field,
    );
};

/**
 * Validates metadata fields and returns the metadata object if valid, or null if invalid.
 *
 * Use this in form submission handlers. If null is returned, call touchInvalidFields()
 * to mark invalid fields and show errors to the user.
 *
 * @param {Array<{key: string, value: string, touched: boolean}>} metadataFields
 * @returns {Object|null} Metadata object if valid, null if validation failed
 */
export const getMetadataIfValid = (metadataFields) => {
    if (hasInvalidKeys(metadataFields)) {
        return null;
    }
    return fieldsToMetadata(metadataFields);
};

/**
 * Returns the validation error message for a metadata field, if any.
 * Only returns an error if the field has been touched (interacted with by the user).
 *
 * Use this function in the MetadataFields component to display field-level errors.
 *
 * @param {Object} field - Metadata field object with key, value, and touched properties
 * @returns {string|null} Error message if field is invalid and touched, null otherwise
 */
export const getFieldError = (field) => {
    if (!field.touched) return null;

    if (isEmptyKey(field.key)) {
        return "Key is required";
    }

    return null;
};
