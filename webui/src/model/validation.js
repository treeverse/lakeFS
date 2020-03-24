
export const isValidBranchName = (from) =>  from && /^[a-zA-Z0-9\\-]{2,}$/.test(from)