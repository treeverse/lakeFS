import React, {
  FC,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useWindowSize } from "usehooks-ts";
import Button from "react-bootstrap/Button";
import Offcanvas from "react-bootstrap/Offcanvas";
import { BsChevronUp, BsChevronDown, BsTrophy } from "react-icons/bs";
import RepoOnboardingChecklist, {
  StepsWithStatus,
} from "./repoOnboardingChecklist";
import { useRefs } from "../../hooks/repo";
import {
  isSampleRepo,
  canUseRepoOnboarding,
  getRepoOnboardingSteps,
} from "../../onboarding/repoOnboardingService";
import { useRouter } from "../../hooks/router";
import RepoOnboardingComplete from "./repoOnboardingComplete";

interface RepoOnboardingChecklistSliderProps {
  showChecklist: (show: boolean) => void;
  blockstoreType: string;
  show: boolean;
  dismissChecklist: () => void;
}

const calculateChecklistTop = (
  checklistBodyRef: React.RefObject<HTMLDivElement>
) => {
  if (!checklistBodyRef.current) {
    return 0;
  }

  const viewportHeight = window.innerHeight;
  const newChecklistTop =
    (viewportHeight - checklistBodyRef.current.getBoundingClientRect().height) /
    2;
  return newChecklistTop;
};

const RepoOnboardingChecklistSlider: FC<RepoOnboardingChecklistSliderProps> = ({
  showChecklist,
  blockstoreType,
  show,
  dismissChecklist,
}) => {
  const { repo } = useRefs();
  const { navigate } = useRouter();
  const [isSampleRepoState, setIsSampleRepoState] = useState(true);
  const [hasPermissions, setHasPermissions] = useState(false);
  const [steps, setSteps] = useState<Array<StepsWithStatus>>([]);
  const [checklistTop, setChecklistTop] = useState(0);
  const checklistBodyRef = useRef<HTMLDivElement>(null);
  const { height: windowHeight } = useWindowSize();

  const toggleShowChecklistHandler = useCallback(() => {
    showChecklist(!show);
  }, [showChecklist, show]);

  const handleOffcanvasEnter = useCallback(() => {
    // make sure the checklist is centered one it's mounted to the DOM
    // (this is needed because the checklist is rendered inside an offcanvas)
    // the formula is: topOfChecklist = (viewportHeight - checklistHeight) / 2
    if (checklistBodyRef.current) {
      setChecklistTop(calculateChecklistTop(checklistBodyRef));
    }
  }, [checklistBodyRef.current, setChecklistTop]);

  useEffect(() => {
    if (checklistBodyRef.current) {
      setChecklistTop(calculateChecklistTop(checklistBodyRef));
    }
  }, [checklistBodyRef.current, windowHeight]);

  useEffect(() => {
    const resolveIsSampleRepo = async () => {
      const isDemo = await isSampleRepo(repo.id, repo.default_branch);
      setIsSampleRepoState(isDemo);
    };
    if (repo) {
      resolveIsSampleRepo();
    }
  }, [repo]);

  useEffect(() => {
    const resolveHasPermissions = async () => {
      const hasPerms = await canUseRepoOnboarding(repo.id, repo.default_branch);
      setHasPermissions(hasPerms);
    };
    if (repo) {
      resolveHasPermissions();
    }
  }, [repo]);

  useEffect(() => {
    const resolveSteps = async () => {
      const steps = getRepoOnboardingSteps(
        repo.id,
        repo.default_branch,
        blockstoreType,
        navigate
      );
      const stepsWithStatus = steps.map((step) => ({
        ...step,
        isCompletedValue: false,
        isLoading: true,
      }));
      setSteps(stepsWithStatus);
    };
    if (repo && hasPermissions && !isSampleRepoState) {
      resolveSteps();
    }
  }, [repo, hasPermissions, isSampleRepoState, blockstoreType, navigate]);

  useEffect(() => {
    const stepStatusResolver = async (
      stepId: string,
      stepCompleteFn: (repoId: string) => Promise<boolean>
    ) => {
      const status = await stepCompleteFn(repo.id);
      setSteps((steps) => {
        const stepIndex = steps.findIndex((step) => step.id === stepId);
        const newSteps = [...steps];
        newSteps[stepIndex] = {
          ...newSteps[stepIndex],
          isCompletedValue: status,
          isLoading: false,
        };
        return newSteps;
      });
    };

    if (steps.length > 0) {
      steps
        .filter((step) => step.isLoading)
        .forEach((step) => {
          if (step.showStep()) {
            stepStatusResolver(step.id, step.isCompleted);
          }
        });
    }
  });

  const [finishedSteps, totalSteps] = useMemo(() => {
    const totalSteps = steps.filter((step) => step.showStep()).length;
    const finishedSteps = steps.reduce(
      (n, current) => n + (current.isCompletedValue ? 1 : 0),
      0
    );
    return [finishedSteps, totalSteps];
  }, [steps]);

  // hide slider if demo repo or user doesn't have permissions
  if (isSampleRepoState || !hasPermissions) {
    return null;
  }

  const closedButtonText =
    steps.length > 0 && steps.some((step) => step.isCompletedValue)
      ? `(${finishedSteps}/${totalSteps}) completed`
      : "Start here";

  return (
    <div className="checklist-container">
      <Button
        className="checklist-slider-button checklist-button-external"
        variant="primary"
        onClick={toggleShowChecklistHandler}
      >
        {closedButtonText}
        {finishedSteps !== totalSteps ? (
          <BsChevronUp style={{ marginLeft: "10px" }} size="16" />
        ) : (
          <BsTrophy style={{ marginLeft: "10px" }} size="16" />
        )}
      </Button>
      <Offcanvas
        show={show}
        backdrop={false}
        scroll={true}
        placement="end"
        style={{ top: checklistTop }}
        className="h-auto checklist-slider"
        onEnter={handleOffcanvasEnter}
      >
        <Button
          className="checklist-slider-button checklist-button-internal"
          variant="primary"
          onClick={toggleShowChecklistHandler}
        >
          {closedButtonText}
          <BsChevronDown style={{ marginLeft: "10px" }} size={16} />
        </Button>
        <Offcanvas.Body
          className="checklist-slider-body"
          ref={checklistBodyRef}
        >
          {finishedSteps === totalSteps ? (
            <RepoOnboardingChecklist steps={steps} />
          ) : (
            <RepoOnboardingComplete dismissChecklist={dismissChecklist} />
          )}
        </Offcanvas.Body>
      </Offcanvas>
    </div>
  );
};

export default RepoOnboardingChecklistSlider;
