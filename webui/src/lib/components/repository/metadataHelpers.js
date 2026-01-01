const isEmptyKey = (key) => !key || key.trim() === '';

/**
 * Checks if a key is a duplicate in the metadata fields array.
 * @param {string} key - The key to check
 * @param {number} index - The index of the current field
 * @param {Array<{key: string, value: string, touched: boolean}>} metadataFields - All fields
 * @returns {boolean} True if the key is a duplicate (exists earlier in the array with a non-empty value)
 */
const isDuplicateKey = (key, index, metadataFields) => {
    if (isEmptyKey(key)) return false;
    const trimmedKey = key.trim();
    return metadataFields.some((f, i) => i < index && f.key.trim() === trimmedKey);
};

/**
 * Checks if a key is invalid (empty or duplicate).
 * @param {string} key - The key to check
 * @param {number} index - The index of the current field (for duplicate checking)
 * @param {Array<{key: string, value: string, touched: boolean}>} metadataFields - All fields (for duplicate checking)
 * @returns {boolean} True if the key is invalid
 */
const isInvalidKey = (key, index = -1, metadataFields = []) => {
    if (isEmptyKey(key)) return true;
    if (index >= 0 && isDuplicateKey(key, index, metadataFields)) return true;
    return false;
};

/**
 * Checks if there are any invalid keys in the metadata fields.
 * @param {Array<{key: string, value: string, touched: boolean}>} metadataFields
 * @returns {boolean} True if there are any invalid keys
 */
export const hasInvalidKeys = (metadataFields) => {
    return metadataFields.some((f, i) => isInvalidKey(f.key, i, metadataFields));
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
    return metadataFields.map((field, index) =>
        isInvalidKey(field.key, index, metadataFields) ? { ...field, touched: true } : field,
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
 * @param {number} index - The index of the current field (for duplicate checking)
 * @param {Array<{key: string, value: string, touched: boolean}>} metadataFields - All fields (for duplicate checking)
 * @returns {string|null} Error message if field is invalid and touched, null otherwise
 */
export const getFieldError = (field, index = -1, metadataFields = []) => {
    if (!field.touched) return null;

    if (isEmptyKey(field.key)) {
        return 'Key is required';
    }

    if (index >= 0 && isDuplicateKey(field.key, index, metadataFields)) {
        return 'Key already exists';
    }

    return null;
};
