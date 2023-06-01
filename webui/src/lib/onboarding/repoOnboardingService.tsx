/* eslint-disable react/jsx-no-target-blank */
import React from "react";
import { OnboardingStep } from "./types";
import { NavigateFunction } from "react-router-dom";
import {
  retention,
  branchProtectionRules,
  objects,
  NotFoundError,
} from "../api";
import { findBranchRegexMatch } from "../api/apiUtils";

const IMPORT_BRANCH_REGEX = /^_(.*)_imported$/;
const HOOKS_PATH = "_lakefs_actions/";
const SAMPLE_REPO_OBJECT_PATH = "lakes.parquet";
const LAKEFS_MAX_PAGE_SIZE = 1000;

export const isSampleRepo = async (
  repo: string,
  defaultBranch: string
): Promise<boolean> => {
  try {
    await objects.head(repo, defaultBranch, SAMPLE_REPO_OBJECT_PATH);
    return true;
  } catch (e) {
    return false;
  }
};

export const canUseRepoOnboarding = async (
  repoId: string,
  defaultBranch: string
): Promise<boolean> => {
  const promises = [
    retention.setGCPolicyPreflight(repoId),
    branchProtectionRules.createRulePreflight(repoId),
    objects.uploadPreflight(repoId, defaultBranch, `${HOOKS_PATH}sample_hook`),
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
): OnboardingStep[] => [
  {
    id: "import-data",
    title: "Import your existing data",
    description: (
      <>
        <p>
          Import your existing data from ${objectStoreName} to your repository
          by creating pointers to it (no copying of data involved).
        </p>
        <p>
          Please see our documentation for information on the{" "}
          <a
            href="https://docs.lakefs.io/howto/import.html#limitations"
            target="_blank"
          >
            limitations of the import process
          </a>{" "}
          and{" "}
          <a
            href="https://docs.lakefs.io/howto/import.html#working-with-imported-data"
            target="_blank"
          >
            working with imported data
          </a>
        </p>
        ,
      </>
    ),
    cta: "Run import",
    onClick: () =>
      navigate(`/repositories/${currentRepo}/objects?importDialog=true`),
    // Disable import step until the new import process and indicator PRs are merged
    // https://github.com/treeverse/lakeFS/pull/5981
    showStep: () => false, // objectStoreName !== "local",
    isCompleted: async () => {
      try {
        const branch = await findBranchRegexMatch(
          currentRepo,
          "_",
          IMPORT_BRANCH_REGEX,
          LAKEFS_MAX_PAGE_SIZE
        );
        if (branch) {
          return true;
        }
        return false;
      } catch (e) {
        if (e instanceof NotFoundError) {
          return false;
        }
        return true;
      }
    },
  },
  {
    id: "garbage-collection",
    title: "Setup garbage collection",
    description: (
      <>
        <p>
          Set up rules to automatically manage when data can be deleted from
          your repository
        </p>
        <p>
          For more information about setting up and running garbage collection
          on your lakeFS repository, see the{" "}
          <a
            href="https://docs.lakefs.io/howto/garbage-collection-committed.html"
            target="_blank"
          >
            documentation
          </a>
        </p>
      </>
    ),
    cta: "Configure",
    onClick: () => navigate(`/repositories/${currentRepo}/settings/retention`),
    showStep: () => true,
    isCompleted: async () => {
      try {
        await retention.getGCPolicy(currentRepo);
        return true;
      } catch (e) {
        if (e instanceof NotFoundError) {
          return false;
        }
        return true;
      }
    },
  },
  {
    id: "branch-protection",
    title: "Set up branch protection",
    description: (
      <>
        <p>You can protect important branches with branch protection rules.</p>
        <p>
          Protected branches cannot be changed directly. Any changes are applied
          only via merging from other branches. This helps protect those
          branches from bugs in your code as well as human error.
        </p>
      </>
    ),
    cta: "Configure",
    onClick: () => navigate(`/repositories/${currentRepo}/settings/branches`),
    showStep: () => true,
    isCompleted: async () => {
      try {
        const rules = await branchProtectionRules.getRules(currentRepo);
        return rules?.length > 0;
      } catch (e) {
        if (e instanceof NotFoundError) {
          return false;
        }
        return true;
      }
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
      try {
        const objectList = await objects.list(
          currentRepo,
          defaultBranch,
          HOOKS_PATH,
          undefined,
          undefined,
          1
        );
        return objectList?.results?.length > 0;
      } catch (e) {
        if (e instanceof NotFoundError) {
          return false;
        }
        return true;
      }
    },
  },
];
