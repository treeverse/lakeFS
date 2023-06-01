import { ReactNode } from 'react';

export type OnboardingStep = {
  id: string;
  title: string;
  description: ReactNode;
  cta: string;
  onClick: (...args: unknown[]) => void;
  showStep: () => boolean | Promise<boolean>;
  isCompleted: (repoName: string) => Promise<boolean>;
};
