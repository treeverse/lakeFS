import React, {FC} from 'react';
import CSS from 'csstype';
import Badge from 'react-bootstrap/Badge';
import Stack from 'react-bootstrap/Stack';
import { FaLock } from 'react-icons/fa';

export const ReadOnlyBadge: FC<{ readOnly: boolean, style: CSS.Properties }> = ({ readOnly, style }) => {
  return readOnly ? (
    <Badge pill bg="secondary" style={style}>
        <Stack direction="horizontal" gap={1}>
            <FaLock />{`Read-only`}
        </Stack>
    </Badge>
  ) : null;
};