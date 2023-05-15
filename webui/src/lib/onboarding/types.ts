export type OnboardingStep = {
  id: string;
  title: string;
  description: string;
  cta: string;
  onClick: (...args: unknown[]) => void;
  showStep: () => boolean | Promise<boolean>;
  isCompleted: (repoName: string) => Promise<boolean>;
};

export type OnBoardingSteps = Array<OnboardingStep>;
