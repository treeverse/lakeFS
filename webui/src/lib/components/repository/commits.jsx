import ButtonGroup from "react-bootstrap/ButtonGroup";
import {ClipboardButton, LinkButton} from "../controls";
import {BrowserIcon, LinkIcon, PackageIcon, PlayIcon} from "@primer/octicons-react";
import Table from "react-bootstrap/Table";
import {MetadataRow, MetadataUIButton} from "../../../pages/repositories/repository/commits/commit/metadata";
import {Link} from "../nav";
import dayjs from "dayjs";
import Card from "react-bootstrap/Card";
import React, {useContext} from "react";
import {AppContext} from "../../hooks/appContext";


const CommitActions = ({ repo, commit }) => {
  const {state} = useContext(AppContext);
  const buttonVariant = state.settings.darkMode ? "outline-light" : "outline-dark";

  return (
    <div>
      <ButtonGroup className="commit-actions">
        <LinkButton
          buttonVariant={buttonVariant}
          href={{pathname: '/repositories/:repoId/objects', params: {repoId: repo.id}, query: {ref: commit.id}}}
          tooltip="Browse commit objects">
          <BrowserIcon/>
        </LinkButton>
        <LinkButton
          buttonVariant={buttonVariant}
          href={{pathname: '/repositories/:repoId/actions', params: {repoId: repo.id}, query: {commit: commit.id}}}
          tooltip="View Commit Action runs">
          <PlayIcon/>
        </LinkButton>
        <ClipboardButton variant={buttonVariant} text={commit.id} tooltip="Copy ID to clipboard"/>
        <ClipboardButton variant={buttonVariant} text={`lakefs://${repo.id}/${commit.id}`} tooltip="Copy URI to clipboard" icon={<LinkIcon/>}/>
        <ClipboardButton variant={buttonVariant} text={`s3://${repo.id}/${commit.id}`} tooltip="Copy S3 URI to clipboard" icon={<PackageIcon/>}/>
      </ButtonGroup>
    </div>
  );
};

const getKeysOrNull = (metadata) => {
  if (!metadata) return null;
  const keys = Object.getOwnPropertyNames(metadata);
  if (keys.length === 0) return null;
  return keys;
};

const CommitMetadataTable = ({ commit }) => {
  const keys = getKeysOrNull(commit.metadata);
  if (!keys) return null;

  return (
    <>
      <Table>
        <thead>
        <tr>
          <th>Metadata Key</th>
          <th>Value</th>
        </tr>
        </thead>
        <tbody>
        {keys.map(key =>
          <MetadataRow metadata_key={key} metadata_value={commit.metadata[key]}/>)}
        </tbody>
      </Table>
    </>
  );
};

const CommitMetadataUIButtons = ({ commit }) => {
  const keys = getKeysOrNull(commit.metadata);
  if (!keys) return null;

  return (
    <>{
      keys.map((key) => <MetadataUIButton metadata_key={key} metadata_value={commit.metadata[key]}/>)
    }</>
  );
};

const CommitLink = ({ repoId, commitId }) => {
  return (
    <>
      <Link href={{
        pathname: '/repositories/:repoId/commits/:commitId',
        params: {repoId, commitId}
      }}>
        <code>{commitId}</code>
      </Link>
      <br/>
    </>
  );
}

const CommitInfo = ({ repo, commit }) => {
  return (
    <Table size="sm" borderless hover>
      <tbody>
      <tr>
        <td><strong>ID</strong></td>
        <td>
          <CommitLink repoId={repo.id} commitId={commit.id}/>
        </td>
      </tr>
      <tr>
        <td><strong>Committer</strong></td>
        <td>{commit.committer}</td>
      </tr>
      <tr>
        <td><strong>Creation Date</strong></td>
        <td>
          {dayjs.unix(commit.creation_date).format("MM/DD/YYYY HH:mm:ss")} ({dayjs.unix(commit.creation_date).fromNow()})
        </td>
      </tr>
      {(commit.parents) ? (
        <tr>
          <td>
            <strong>Parents</strong></td>
          <td>
            {commit.parents.map(cid => (
              <CommitLink key={cid} repoId={repo.id} commitId={cid}/>
            ))}
          </td>
        </tr>
      ) : <></>}
      </tbody>
    </Table>
  );
};

export const CommitMessage = ({ commit }) =>
    commit.message?.length ?
        <span>{commit.message}</span> : <span className="text-muted">(No commit message)</span>;

export const CommitInfoCard = ({ repo, commit, bare = false }) => {
  const content = (
    <>
        <div className="d-flex">
          <div className="flex-grow-1">
            <h4><CommitMessage commit={commit}/></h4>
          </div>
          <div>
            <CommitActions repo={repo} commit={commit}/>
          </div>
        </div>

      <div className="mt-4">
        <CommitInfo repo={repo} commit={commit}/>
        <CommitMetadataUIButtons commit={commit}/>
        <div className="mt-3">
          <CommitMetadataTable commit={commit}/>
        </div>
      </div>
    </>
  );
  if (bare) return content;
  return (
    <Card>
      <Card.Body>
        {content}
      </Card.Body>
    </Card>
  )
}
