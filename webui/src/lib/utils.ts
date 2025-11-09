interface User {
  id: string;
  email: string;
  friendly_name: string;
}

export const resolveUserDisplayName = (user: User): string => {
  if (!user) return "";
  if (user?.email?.length) return user.email;
  if (user?.friendly_name?.length) return user.friendly_name;
  return user.id;
};

export const ROUTES = {
    LOGIN: "/auth/login",
    OIDC_PREFIX: "/oidc/",
    SAML_PREFIX: "/sso/",
    SETUP: "/setup",
    REPOSITORIES: "/repositories",
} as const;

export const isPublicAuthRoute = (path: string) =>
    path === ROUTES.LOGIN || path.startsWith(ROUTES.OIDC_PREFIX) || path.startsWith(ROUTES.SAML_PREFIX);

export const getCurrentRelativeUrl = () =>
    window.location.pathname + (window.location.search || "") + (window.location.hash || "");

export const normalizeNext = (raw?: string | null) => {
    const n = (raw ?? "").trim();
    const safe = n.startsWith("/") ? n : "";
    return safe && safe !== "/" ? safe : ROUTES.REPOSITORIES;
};
