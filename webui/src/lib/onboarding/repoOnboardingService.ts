import { OnBoardingSteps } from "./types";
import { NavigateFunction } from "react-router-dom";
import { branches, retention, branchProtectionRules, objects } from "../api";

const IMPORT_BRANCH_REGEX = /^_(.*)_imported$/;
const HOOKS_PATH = "_lakefs_actions/";
const SAMPLE_REPO_FILE_NAME = "lakes.parquet";

// generate a random file name to avoid collisions for the upload preflight check
const generateRandomFileName = (): string => {
  const randomString = Math.random().toString(36).substring(2, 15);
  return `sample_${randomString}.yaml`;
};

export const isSampleRepo = async (
  repo: string,
  defaultBranch: string,
): Promise<boolean> => {
  const objectList = await objects.list(repo, defaultBranch, "");
  return objectList.results.some(
    // when the API client is typed, we can remove the eslint-disable
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (object: any) => object?.path === SAMPLE_REPO_FILE_NAME
  );
};

export const canUseRepoOnboarding = async (
  repoId: string,
  defaultBranch: string,
): Promise<boolean> => {
  const promises = [
    retention.setGCPolicyPreflight(repoId),
    branchProtectionRules.createRulePreflight(repoId),
    objects.list(
      repoId,
      defaultBranch,
      `${HOOKS_PATH}${generateRandomFileName()}`
    ),
  ];
  
  try {
    await Promise.all(promises);
    return true;
  } catch (e) {
    return false;
  }
};

export const getRepoOnboardingSteps = (
  currentRepo: string,
  defaultBranch: string,
  objectStoreName: string,
  navigate: NavigateFunction
): OnBoardingSteps => [
  {
    id: "import-data",
    title: "Import your existing data",
    description: `Import your existing data from ${objectStoreName} to your repository by creating pointers to it (no copying of data involved)`,
    cta: "Run import",
    onClick: () =>
      navigate(`/repositories/${currentRepo}/objects?importDialog=true`),
    showStep: () => objectStoreName !== "local",
    isCompleted: async () => {
      const repoBranches = await branches.list(currentRepo);
      // when the API client is typed, we can remove the eslint-disable
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return repoBranches.results.some((branch: any) =>
        IMPORT_BRANCH_REGEX.test(branch.id)
      );
    },
  },
  {
    id: "garbage-collection",
    title: "Setup garbage collection",
    description:
      "Setup rules to automatically manage when data can be deleted from your repository",
    cta: "Configure",
    onClick: () => navigate(`/repositories/${currentRepo}/settings/retention`),
    showStep: () => true,
    isCompleted: async () => {
      try {
        await retention.getGCPolicy(currentRepo);
        return true;
      } catch (e) {
        return false;
      }
    },
  },
  {
    id: "branch-protection",
    title: "Set up branch protection",
    description:
      "Add a quality gate to ensure your most important branches pass quality, compliance, or privacy checks",
    cta: "Configure",
    onClick: () => navigate(`/repositories/${currentRepo}/settings/branches`),
    showStep: () => true,
    isCompleted: async () => {
      const rules = await branchProtectionRules.getRules(currentRepo);
      return rules.length > 0;
    },
  },
  {
    id: "hooks",
    title: "Set up hooks",
    description:
      "Create simple hooks to validate new data meets certain requirements: schema validation, metadata registration, and more.",
    cta: "Configure",
    onClick: () =>
      window.open("https://docs.lakefs.io/hooks/overview.html", "_blank"),
    showStep: () => true,
    isCompleted: async () => {
      const objectList = await objects.list(currentRepo, defaultBranch, "");
      return objectList.results.some(
        // when the API client is typed, we can remove the eslint-disable
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (object: any) => object?.path === HOOKS_PATH
      );
    },
  },
];
