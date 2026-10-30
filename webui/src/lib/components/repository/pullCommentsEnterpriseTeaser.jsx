import React from 'react';
import Card from 'react-bootstrap/Card';
import { ArrowUpRightIcon, CommentDiscussionIcon } from '@primer/octicons-react';

const placeholderComments = [
    { author: 'data-engineer', lines: [90, 60] },
    { author: 'reviewer', lines: [75] },
    { author: 'ml-researcher', lines: [85, 45] },
];

export const PullCommentsEnterpriseTeaser = () => (
    <Card className="pull-comments-teaser mt-4">
        <Card.Header className="d-flex align-items-center gap-2">
            <CommentDiscussionIcon /> Comments
        </Card.Header>
        <Card.Body className="position-relative">
            <div className="pull-comments-teaser-placeholder" aria-hidden="true">
                {placeholderComments.map(({ author, lines }) => (
                    <div className="pull-comments-teaser-comment" key={author}>
                        <div className="pull-comments-teaser-avatar" />
                        <div className="flex-grow-1">
                            <div className="pull-comments-teaser-author">{author}</div>
                            {lines.map((width, i) => (
                                <div className="pull-comments-teaser-line" style={{ width: `${width}%` }} key={i} />
                            ))}
                        </div>
                    </div>
                ))}
                <div className="pull-comments-teaser-input" />
            </div>
            <div className="pull-comments-teaser-overlay">
                <CommentDiscussionIcon size={32} className="pull-comments-teaser-icon" />
                <div className="pull-comments-teaser-title">Collaborate on pull requests</div>
                <div className="pull-comments-teaser-description">
                    Discuss changes, share feedback, and collaborate with your team using comments in lakeFS Enterprise.
                </div>
                <a
                    href="https://lakefs.io/enterprise/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="btn btn-primary mt-3 d-inline-flex align-items-center gap-2"
                >
                    Explore lakeFS Enterprise <ArrowUpRightIcon />
                </a>
            </div>
        </Card.Body>
    </Card>
);
