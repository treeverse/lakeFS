import jwt from 'jsonwebtoken';

interface License {
  organization_id: string;
  installation_id: string;
  description: string;
  issuer: string;
  audience: string[];
  issue_date: string;
  expiry_date: string;
}

export function createDate(days: number = 0, hours: number = 0): Date {
    const date = new Date();
    date.setDate(date.getDate() + days);
    date.setHours(date.getHours() + hours);
    return date;
}

export function createLicenseToken(expiryDate: Date): string {
  const payload: License = {
    organization_id: "treeverse-dev",
    installation_id: "",
    description: "Treeverse's temporary development environment",
    issuer: "Treeverse Inc.",
    audience: [
      "lakeFS Enterprise User"
    ],
    issue_date: new Date().toISOString(),
    expiry_date: expiryDate.toISOString()
  };

  return jwt.sign(payload, 'test-secret');
} 