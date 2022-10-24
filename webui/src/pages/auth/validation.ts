type ValidationError = string;

export const INVALID_USER_NAME_ERROR_MESSAGE: ValidationError =
  "User name cannot include the % sign";
export const INVALID_GROUP_NAME_ERROR_MESSAGE: ValidationError =
  "Group name cannot include the % sign";
export const INVALID_POLICY_ID_ERROR_MESSAGE: ValidationError =
  "Policy ID cannot include the % sign";

interface ValidationResult {
  isValid: boolean;
  errorMessage?: string;
}

type ValidationFunction = (value: string) => ValidationResult;

export const disallowPercentSign =
  (errorMessage: ValidationError): ValidationFunction =>
  (value: string): ValidationResult => {
    if (value.includes("%")) {
      return {
        isValid: false,
        errorMessage: errorMessage,
      };
    }

    return {
      isValid: true,
    };
  };
