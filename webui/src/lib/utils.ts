interface User {
  id: string;
  email: string;
  friendly_name: string;
}

export const resolveDisplayName = (user: User): string => {
  if (!user) return "";
  if (user?.email?.length) return user.email;
  if (user?.friendly_name?.length) return user.friendly_name;
  return user.id;
};
