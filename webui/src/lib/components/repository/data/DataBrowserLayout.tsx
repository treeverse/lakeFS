import React, { useState, useCallback, useRef, useEffect } from 'react';

import { DataBrowserProvider } from './DataBrowserContext';
import { DataAccordion } from './DataAccordion';
import { ObjectViewerPanel } from './ObjectViewerPanel';

interface DataBrowserLayoutProps {
    repo: { id: string; read_only?: boolean };
    reference: { id: string; type: string };
    config: {
        pre_sign_support?: boolean;
        pre_sign_support_ui?: boolean;
    };
    initialPath?: string;
    onNavigate?: (path: string) => void;
    refreshToken?: boolean;
}

const MIN_LEFT_WIDTH = 200;
const MAX_LEFT_WIDTH_PERCENT = 60;
const DEFAULT_LEFT_WIDTH = 300;
const STORAGE_KEY = 'lakefs-data-browser-left-width';

export const DataBrowserLayout: React.FC<DataBrowserLayoutProps> = ({
    repo,
    reference,
    config,
    initialPath,
    onNavigate,
    refreshToken,
}) => {
    const containerRef = useRef<HTMLDivElement>(null);
    const [leftWidth, setLeftWidth] = useState<number>(() => {
        const stored = localStorage.getItem(STORAGE_KEY);
        return stored ? parseInt(stored, 10) : DEFAULT_LEFT_WIDTH;
    });
    const [isDragging, setIsDragging] = useState(false);

    const handleMouseDown = useCallback((e: React.MouseEvent) => {
        e.preventDefault();
        setIsDragging(true);
    }, []);

    const handleMouseMove = useCallback(
        (e: MouseEvent) => {
            if (!isDragging || !containerRef.current) return;

            const containerRect = containerRef.current.getBoundingClientRect();
            const newWidth = e.clientX - containerRect.left;
            const maxWidth = containerRect.width * (MAX_LEFT_WIDTH_PERCENT / 100);

            const clampedWidth = Math.max(MIN_LEFT_WIDTH, Math.min(newWidth, maxWidth));
            setLeftWidth(clampedWidth);
        },
        [isDragging],
    );

    const handleMouseUp = useCallback(() => {
        if (isDragging) {
            setIsDragging(false);
            localStorage.setItem(STORAGE_KEY, leftWidth.toString());
        }
    }, [isDragging, leftWidth]);

    useEffect(() => {
        if (isDragging) {
            document.addEventListener('mousemove', handleMouseMove);
            document.addEventListener('mouseup', handleMouseUp);
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
        }

        return () => {
            document.removeEventListener('mousemove', handleMouseMove);
            document.removeEventListener('mouseup', handleMouseUp);
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
        };
    }, [isDragging, handleMouseMove, handleMouseUp]);

    const handlePathChange = useCallback(
        (path: string) => {
            if (onNavigate) {
                onNavigate(path);
            }
        },
        [onNavigate],
    );

    return (
        <DataBrowserProvider
            initialPath={initialPath}
            onPathChange={handlePathChange}
            externalRefreshToken={refreshToken}
        >
            <div className="data-browser-layout" ref={containerRef}>
                <div className="data-browser-panels">
                    <div className="left-panel" style={{ width: leftWidth }}>
                        <div className="left-panel-content">
                            <DataAccordion
                                repo={repo}
                                reference={reference}
                                config={config}
                                onNavigate={onNavigate}
                                initialPath={initialPath}
                            />
                        </div>
                    </div>
                    <div
                        className={`resize-handle ${isDragging ? 'dragging' : ''}`}
                        onMouseDown={handleMouseDown}
                        title="Drag to resize"
                    >
                        <div className="resize-handle-bar" />
                    </div>
                    <div className="right-panel">
                        <div className="right-panel-content">
                            <ObjectViewerPanel repo={repo} reference={reference} config={config} />
                        </div>
                    </div>
                </div>
            </div>
        </DataBrowserProvider>
    );
};
