import React, { useEffect, useState, useCallback } from 'react';
import { useOutletContext } from 'react-router-dom';
import dayjs from 'dayjs';
import { BrowserIcon, LinkIcon, PlayIcon, TagIcon } from '@primer/octicons-react';

import { commits, tags, MAX_LISTING_AMOUNT } from '../../../../lib/api';
import Badge from 'react-bootstrap/Badge';
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import Tooltip from 'react-bootstrap/Tooltip';
import Button from 'react-bootstrap/Button';
import ButtonGroup from 'react-bootstrap/ButtonGroup';
import Card from 'react-bootstrap/Card';
import Form from 'react-bootstrap/Form';
import ListGroup from 'react-bootstrap/ListGroup';

import {
    ActionGroup,
    ActionsBar,
    ClipboardButton,
    AlertError,
    LinkButton,
    Loading,
    RefreshButton,
} from '../../../../lib/components/controls';
import { CommitMessage } from '../../../../lib/components/repository/commits';
import { useRefs } from '../../../../lib/hooks/repo';
import { useAPIWithPagination } from '../../../../lib/hooks/api';
import { Paginator } from '../../../../lib/components/pagination';
import RefDropdown from '../../../../lib/components/repository/refDropdown';
import { Link } from '../../../../lib/components/nav';
import { useRouter } from '../../../../lib/hooks/router';
import { RepoError } from '../error';
import { RefTypeBranch } from '../../../../constants';

const MAX_TAG_PAGES = 3; // Limit number of calls to have responsive UI and not overload the server with calls
const MAX_TAGS_FOR_DISPLAY = MAX_TAG_PAGES * MAX_LISTING_AMOUNT;

// Fetch all tags up to MAX_TAG_PAGES pages
// Returns { tags: [...], limitExceeded: boolean }
const fetchAllTags = async (repoId) => {
    const allTags = [];
    const iterator = tags.listAll(repoId);

    for (let page = 0; page < MAX_TAG_PAGES; page++) {
        const { page: results, done } = await iterator.next();
        allTags.push(...results);
        if (done) {
            return { tags: allTags, limitExceeded: false };
        }
    }

    // More than MAX_TAG_PAGES pages of tags, don't display any
    // and report limit exceeded
    return { tags: [], limitExceeded: true };
};

// Build a map of commit ID to array of tag names
const buildCommitTagsMap = (tagsList) => {
    const map = new Map();
    for (const tag of tagsList) {
        const commitId = tag.commit_id;
        if (!map.has(commitId)) {
            map.set(commitId, []);
        }
        map.get(commitId).push(tag.id);
    }
    return map;
};

const CommitWidget = ({
    repo,
    commit,
    commitTags = [],
    revertMode = false,
    isSelected = false,
    onToggleSelect = null,
}) => {
    const buttonVariant = 'light';

    return (
        <ListGroup.Item>
            <div className="clearfix">
                <div className="float-start">
                    {revertMode && (
                        <Form.Check
                            type="checkbox"
                            checked={isSelected}
                            onChange={() => onToggleSelect && onToggleSelect(commit.id)}
                            className="float-start me-2 mt-1"
                            inline
                        />
                    )}
                    <div className={revertMode ? 'd-inline-block' : ''}>
                        <h6>
                            <Link
                                href={{
                                    pathname: '/repositories/:repoId/commits/:commitId',
                                    params: { repoId: repo.id, commitId: commit.id },
                                }}
                            >
                                <CommitMessage commit={commit} />
                            </Link>
                            {commitTags.map((tagName) => (
                                <Link
                                    key={tagName}
                                    href={{
                                        pathname: '/repositories/:repoId/objects',
                                        params: { repoId: repo.id },
                                        query: { ref: tagName },
                                    }}
                                >
                                    <Badge bg="secondary" className="ms-2">
                                        <TagIcon size={12} className="me-1" />
                                        {tagName}
                                    </Badge>
                                </Link>
                            ))}
                        </h6>
                        <p>
                            <small>
                                <strong>{commit.committer}</strong> committed at{' '}
                                <strong>{dayjs.unix(commit.creation_date).format('MM/DD/YYYY HH:mm:ss')}</strong> (
                                {dayjs.unix(commit.creation_date).fromNow()})
                            </small>
                        </p>
                    </div>
                </div>
                <div className="float-end">
                    <ButtonGroup className="commit-actions mt-1">
                        <LinkButton
                            buttonVariant={buttonVariant}
                            href={{
                                pathname: '/repositories/:repoId/commits/:commitId',
                                params: { repoId: repo.id, commitId: commit.id },
                            }}
                        >
                            <code>{commit.id.substr(0, 16)}</code>
                        </LinkButton>
                        <LinkButton
                            buttonVariant={buttonVariant}
                            href={{
                                pathname: '/repositories/:repoId/actions',
                                query: { commit: commit.id },
                                params: { repoId: repo.id },
                            }}
                            tooltip="View Commit Action runs"
                        >
                            <PlayIcon />
                        </LinkButton>
                        <ClipboardButton
                            variant={buttonVariant}
                            text={`lakefs://${repo.id}/${commit.id}`}
                            tooltip="Copy URI to clipboard"
                            icon={<LinkIcon />}
                        />
                        <LinkButton
                            buttonVariant={buttonVariant}
                            href={{
                                pathname: '/repositories/:repoId/objects',
                                params: { repoId: repo.id },
                                query: { ref: commit.id },
                            }}
                            tooltip="Browse objects at this commit"
                        >
                            <BrowserIcon />
                        </LinkButton>
                    </ButtonGroup>
                </div>
            </div>
        </ListGroup.Item>
    );
};

const CommitsBrowser = ({ repo, reference, after, onPaginate, onSelectRef }) => {
    const router = useRouter();
    const [refresh, setRefresh] = useState(true);
    const [revertMode, setRevertMode] = useState(false);
    const [selectedCommits, setSelectedCommits] = useState(new Set());
    const [commitTagsMap, setCommitTagsMap] = useState(new Map());
    const [tagsLimitExceeded, setTagsLimitExceeded] = useState(false);
    const [tagsError, setTagsError] = useState(null);

    const { results, error, loading, nextPage } = useAPIWithPagination(async () => {
        return commits.log(repo.id, reference.id, after);
        // TODO: Review and remove this eslint-disable once dependencies are validated
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [repo.id, reference.id, refresh, after]);

    // Load tags once when repo changes or on refresh
    const loadTags = useCallback(async () => {
        try {
            const { tags: allTags, limitExceeded } = await fetchAllTags(repo.id);
            setTagsLimitExceeded(limitExceeded);
            setTagsError(null);
            const tagsMap = limitExceeded ? new Map() : buildCommitTagsMap(allTags);
            setCommitTagsMap(tagsMap);
        } catch (err) {
            // On error, just don't show tags
            setCommitTagsMap(new Map());
            setTagsLimitExceeded(false);
            setTagsError(err);
        }
        // TODO: Review and remove this eslint-disable once dependencies are validated
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [repo.id, refresh]);

    useEffect(() => {
        loadTags();
    }, [loadTags]);

    const toggleCommitSelection = (commitId) => {
        const newSelected = new Set(selectedCommits);
        if (newSelected.has(commitId)) {
            newSelected.delete(commitId);
        } else {
            newSelected.add(commitId);
        }
        setSelectedCommits(newSelected);
    };

    const handleRevertClick = () => {
        setRevertMode(!revertMode);
        setSelectedCommits(new Set()); // Clear selection when toggling mode
    };

    const handleContinue = () => {
        // Maintain commit order (not click order) by filtering results
        const commitIds = results
            .filter((commit) => selectedCommits.has(commit.id))
            .map((commit) => commit.id)
            .join(',');
        router.push({
            pathname: '/repositories/:repoId/branches/:branchId/revert',
            params: { repoId: repo.id, branchId: reference.id },
            query: { commits: commitIds },
        });
    };

    if (loading) return <Loading />;
    if (error) return <AlertError error={error} />;

    const isBranch = reference && reference.type === RefTypeBranch;

    return (
        <div className="mb-5">
            <ActionsBar>
                <ActionGroup orientation="left">
                    <RefDropdown
                        repo={repo}
                        selected={reference ? reference : null}
                        withCommits={true}
                        withWorkspace={false}
                        selectRef={onSelectRef}
                    />
                </ActionGroup>

                <ActionGroup orientation="right">
                    {isBranch && revertMode && selectedCommits.size > 0 && (
                        <Button variant="success" onClick={handleContinue}>
                            Continue ({selectedCommits.size})
                        </Button>
                    )}
                    {isBranch && (
                        <Button variant={revertMode ? 'secondary' : 'light'} onClick={handleRevertClick}>
                            {revertMode ? 'Cancel' : 'Revert'}
                        </Button>
                    )}
                    <RefreshButton
                        onClick={() => {
                            setRefresh(!refresh);
                        }}
                    />
                </ActionGroup>
            </ActionsBar>

            {tagsError && <AlertError error={tagsError} onDismiss={() => setTagsError(null)} className="mt-3" />}

            {tagsLimitExceeded && (
                <OverlayTrigger
                    placement="right"
                    overlay={
                        <Tooltip>Too many tags to display ({MAX_TAGS_FOR_DISPLAY.toLocaleString()}+ limit)</Tooltip>
                    }
                >
                    <small className="text-muted d-inline-block mb-2" style={{ cursor: 'help' }}>
                        <TagIcon /> Tag labels hidden
                    </small>
                </OverlayTrigger>
            )}

            <Card>
                <ListGroup variant="flush">
                    {results.map((commit) => (
                        <CommitWidget
                            key={commit.id}
                            repo={repo}
                            commit={commit}
                            commitTags={commitTagsMap.get(commit.id) || []}
                            revertMode={revertMode}
                            isSelected={selectedCommits.has(commit.id)}
                            onToggleSelect={toggleCommitSelection}
                        />
                    ))}
                </ListGroup>
            </Card>
            <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after} />
        </div>
    );
};

const CommitsContainer = () => {
    const router = useRouter();
    const { after } = router.query;
    const { repo, reference, loading, error } = useRefs();

    if (loading) return <Loading />;
    if (error) return <RepoError error={error} />;

    const params = { repoId: repo.id };

    return (
        <CommitsBrowser
            repo={repo}
            reference={reference}
            onSelectRef={(ref) =>
                router.push({
                    pathname: `/repositories/:repoId/commits`,
                    query: { ref: ref.id },
                    params,
                })
            }
            after={after ? after : ''}
            onPaginate={(after) =>
                router.push({
                    pathname: `/repositories/:repoId/commits`,
                    query: { ref: reference.id, after },
                    params,
                })
            }
        />
    );
};

const RepositoryCommitsPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage('commits'), [setActivePage]);
    return <CommitsContainer />;
};

export default RepositoryCommitsPage;
